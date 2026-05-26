import { in_Message, in_sendPrivateMessage } from "../types/roturd_incoming";
import {
	out_ErrorPacket,
	out_Message,
	out_PrivateMessagePacket,
	out_SuccessPacket
} from "../types/roturd_outgoing";
import { Environment, SocketConnection } from "../types/worker";
import { sleep } from "./time";

export default class RoturLibrary {
	#socketConnection?: SocketConnection;
	#_nextResponder = 0;
	get #nextResponder() {
		// must be string
		return String(this.#_nextResponder++);
	}

	awaitingResponses: Record<
		string,
		{
			promise: Promise<out_Message>;
			resolve: (data: out_Message) => void;
			reject: (reason?: any) => void;
		}
	> = {};

	#msg<T extends out_Message>(message: in_Message): Promise<T> {
		const sock = this.#socketConnection;
		if (!sock) return new Promise(() => {});

		const responder = this.#nextResponder;
		sock.sendMessage({ ...message, responder });

		const obj: RoturLibrary["awaitingResponses"][0] = {
			// @ts-expect-error
			promise: undefined,
			resolve: () => {},
			reject: () => {}
		};
		obj.promise = new Promise<T>((resolve, reject) => {
			// @ts-expect-error
			obj.resolve = resolve;
			obj.reject = reject;
		});

		this.awaitingResponses[responder] = obj;

		// @ts-expect-error
		return obj.promise;
	}

	constructor(public env: Environment) {}

	async init() {
		const socketDirectory = "/data/rotur/rotur.sock";
		const serverDirectory = "/bin/roturd.js";

		if (!(await this.env.fs.exists(socketDirectory))) {
			await this.env.execute(serverDirectory);

			// wait to give server time to wake up
			await sleep(1000);
		}

		const socket = await this.env.sockets.connectToSocket(socketDirectory);
		this.#socketConnection = socket;

		socket.onMessage = async (msg: out_Message) => {
			switch (msg.intent) {
				case "success": {
					const obj = this.awaitingResponses[msg.responder];
					obj.resolve(msg);
					break;
				}

				case "error": {
					this.env.error(`${msg.responder}: ${msg.message}`);

					const obj = this.awaitingResponses[msg.responder];
					obj.reject(msg.message);
					break;
				}
				case "privateMessage": {
					this.onMessage?.(msg);
					break;
				}
				case "needToken": {
					const token = await getRoturToken(this.env);

					this.#socketConnection?.sendMessage({
						intent: "token",
						token
					});
					break;
				}

				default:
					this.env.warn(
						`Unknown packet from 'roturd' server: ${JSON.stringify(msg)}`
					);
			}
		};

		await this.#msg({ intent: "awaitInit" });
	}

	onMessage?: (msg: out_PrivateMessagePacket) => any;

	async listUsers() {
		type UserListPacket = out_SuccessPacket<
			{ username: string; rotur: string | null }[]
		>;

		return (
			await this.#msg<UserListPacket>({
				intent: "listUsers"
			})
		).message;
	}

	async claimPort(name: string) {
		const resp = await this.#msg({ intent: "claimPort", port: name });

		if (resp?.intent == "error") throw new Error(resp.message);
	}

	async releasePort(name: string) {
		const resp = await this.#msg({ intent: "releasePort", port: name });

		if (resp?.intent == "error") throw new Error(resp.message);
	}

	async getUser() {
		const resp = await this.#msg<
			| out_SuccessPacket<{ username: string; rotur: string | null }>
			| out_ErrorPacket
		>({
			intent: "getUser"
		});

		if (resp?.intent == "error") throw new Error(resp.message);

		return resp.message;
	}

	async sendPrivateMessage(
		targetUser: string,
		localPort: string,
		remotePort: string,
		payload: string
	) {
		const msg: in_sendPrivateMessage = {
			intent: "sendPrivateMessage",
			outgoingPort: localPort,
			payload: payload,
			targetPort: remotePort,
			targetSocketUser: targetUser
		};

		const outcome = await this.#msg<out_SuccessPacket | out_ErrorPacket>(
			msg
		);

		if (outcome.intent == "error") throw new Error(outcome.message);
	}
}
export async function getRoturToken(env: Environment) {
	await env.fs.mkdir("/data/rotur");
	const tokenFile = "/data/rotur/token.json";

	try {
		const tokenJsonFile = await env.fs.readFile<{ token: string }>(
			tokenFile,
			"json"
		);

		if (tokenJsonFile?.token) return tokenJsonFile?.token;
	} catch (e) {}

	const { code } = await env.network.request<{ code: string }>(
		"get",
		"https://api.rotur.dev/link/code",
		"json"
	);

	env.print(
		`Please open 'https://rotur.dev/link' and enter the code ${code}.`
	);

	await env.input("Press enter when done.");

	type LinkResponse =
		| { linked: false; token: "" }
		| { linked: true; token: string };

	const auth = await env.network.request<LinkResponse>(
		"get",
		`https://api.rotur.dev/link/user?code=${code}`,
		"json"
	);

	if (!auth.linked) {
		throw new Error("Link not completed by user.");
	} else {
		await env.fs.writeFile(
			tokenFile,
			JSON.stringify({ token: auth.token })
		);

		return auth.token;
	}
}
