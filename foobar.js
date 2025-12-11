import IrisApi from "./Api/IrisApi.js";
import * as Log from "../../app/util/log.js";
import WebSocket from "ws";
import TwilioConnection from "./Connections/TwilioConnection.js";
import AudioProcessor from "../util/AudioProcessor.js";
import AssistantConnection from "./Connections/AssistantConnection.js";
import fs from "node:fs";
import path from "node:path";

// Initialize once when you start receiving audio
let audioWriteStream = null;

function initializeAudioRecording() {
  if (!audioWriteStream) {
    const filename = `audio_${Date.now()}.ulaw`;
    const filepath = "./audio/" + filename;

    // Ensure directory exists
    const dir = path.dirname(filepath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    audioWriteStream = fs.createWriteStream(filepath);
    console.log(`Recording to: ${filepath}`);
  }
}
function stopRecording() {
  if (audioWriteStream) {
    audioWriteStream.end();
    audioWriteStream = null;
    console.log("Recording finished");
  }
}

/**
 * Class that is constructed once per flow
 * Handles call flow from the end of flow-start to the start of flow-stop
 */
export default class AssistantStreamHandler {
  /**
   * Constructor
   *
   * @param   {WebSocket}     connection
   * @param   {string}        url
   */
  constructor(connection, url) {
    Log.debug("WS: New Connection");
    this.callEnded = false;
    this.transcript = [];
    this.decoder = new TextDecoder("utf8");

    let url_parts = url.split("/");
    let call_uuid = url_parts[2];

    if (call_uuid === null) {
      Log.error("WS: No UUID Provided");
      this.closeConnection();
    }

    this.audioProcessor = new AudioProcessor();
    this.twilio = new TwilioConnection(
      connection,
      this.handleTwilioEvent.bind(this),
      this.handleStreamComplete.bind(this)
    );
    this.assistant = new AssistantConnection(
      this.handleAssistantEvent.bind(this)
    );
  }

  /**
   * Relayed Twilio Event
   *
   * @param   {string}  event
   * @param   {any}     data
   *
   * @return  {void}
   */
  async handleTwilioEvent(event, data) {
    if (!this.assistant?.isReady) {
      return;
    }

    // Media

    // Call this once when you start receiving media events
    initializeAudioRecording();

    if (event === "media") {
      const twilioAudioData = data.media.payload;
      const audioBuffer = Buffer.from(twilioAudioData, "base64");

      // Write chunk to stream
      if (audioWriteStream) {
        audioWriteStream.write(audioBuffer);
      }

      this.assistant.listen(audioBuffer);
    }
  }

  /**
   * Relayed Assistant Event
   *
   * @param   {string}  event
   * @param   {any}     data
   *
   * @return  {void}
   */
  async handleAssistantEvent(event, data) {
    if (
      !this.twilio?.isReady ||
      this.twilio.connection.readyState !== WebSocket.OPEN
    ) {
      return;
    }

    // Handle Audio
    if (event === "media") {
      const formattedAudioData = JSON.stringify({
        event: "media",
        streamSid: this.twilio.streamSid,
        media: { payload: data },
      });
      this.twilio.connection.send(formattedAudioData);
    }

    // Handle Audio Interruption - NEW
    if (event === "stop-audio") {
      Log.debug(
        "Assistant: Stopping current audio playback due to interruption"
      );
      const clearAudioMessage = JSON.stringify({
        event: "clear",
        streamSid: this.twilio.streamSid,
      });
      this.twilio.connection.send(clearAudioMessage);
    }

    // Handle a Control Message
    if (event === "control") {
      // Transfer Override Keyword for DEV only: Helicopter
      if (
        process.env.APP_DEBUG === "true" &&
        data.type === "transcript" &&
        data.transcript.includes("elicopter")
      ) {
        Log.debug("Livekit: Transfer Override");
        await this.handleStreamComplete("transfer");
        return;
      }

      // Conversation Update
      if (data.type === "conversation-update") {
        this.transcript = data.messages;

        Log.debug(
          "Livekit: Conversation",
          this.transcript[this.transcript.length - 1]
        );
      }

      // Function
      if (data.type === "model-output" && data.output[0].type === "function") {
        let function_name = data.output[0].function.name;
        Log.debug(`Livekit: FUNCTION CALLED: ${function_name}`);

        if (function_name.includes("ws-rebuttal-tool")) {
          let function_arguments = data.output[0].function.arguments;
          let handoff_result = JSON.parse(function_arguments)["rebuttal"];

          Log.debug("Livekit: REBUTTAL", handoff_result);
          await this.handleStreamComplete("handoff", handoff_result);
          return;
        }

        if (function_name.includes("ws-transfer-call")) {
          await this.handleStreamComplete("transfer");
          return;
        }

        if (function_name.includes("ws-end-call")) {
          await this.handleStreamComplete("break");
          return;
        }
      }
    }

    // Handle ID Exchange
    if (event === "id") {
      Log.debug("Livekit: Id Pass", data);
      // this.twilio.callData['Livekit_call_id'] = data['call_id'];
      // this.twilio.callData['Livekit_assistant_id'] = data['assistant_id'];
      return;
    }
  }

  /**
   * Reach out to IRIS once the stream had completed
   *
   * @param {string} result
   * @param {string|null} handoff_result
   *
   * @return {void}
   */
  async handleStreamComplete(result, handoff_result = null) {
    Log.info(`Livekit: Stream Result: ${result}`);
    stopRecording();
    if (this.callEnded) {
      return;
    }

    this.callEnded = true;
    if (result === "handoff") {
      Log.debug(
        `WS: Handing Off Stream: ${handoff_result}`,
        this.twilio.callData
      );
      await IrisApi.handoffStream(
        handoff_result,
        this.twilio.irisUrl,
        this.transcript,
        this.twilio.callData
      );
    } else {
      Log.debug(
        `WS: Ending Stream with Result: ${result}`,
        this.twilio.callData
      );
      await IrisApi.endStream(
        result,
        this.twilio.irisUrl,
        this.transcript,
        this.twilio.callData
      );
    }

    if (this.livekit) {
      this.livekit.isReady = false;
      this.livekit.cleanup();
      this.livekit = undefined;
    }

    if (this.twilio) {
      this.twilio.isReady = false;
      this.twilio.closeConnection();
      this.twilio = undefined;
    }
  }
}
