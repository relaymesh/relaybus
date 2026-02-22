const { HttpPublisher, HttpSubscriber } = require("../../sdk/http/typescript/dist/index.js");

async function main() {
  const mode = process.argv[2] || "sub";
  const port = Number(process.env.HTTP_PORT || 8088);
  const endpoint = process.env.HTTP_ENDPOINT || `http://localhost:${port}/{topic}`;
  const topic = process.env.TOPIC || "relaybus.alpha";

  if (mode === "sub") {
    let server;
    let timeout;
    const subscriber = new HttpSubscriber({
      onMessage: (msg) => {
        console.log(`received id=${msg.id} topic=${msg.topic} payload=${msg.payload.toString()}`);
        if (timeout) {
          clearTimeout(timeout);
        }
        if (server) {
          server.close();
        }
      }
    });

    server = await subscriber.listen({ port });
    timeout = setTimeout(() => {
      console.error("timeout waiting for message");
      server.close();
      process.exit(1);
    }, 30000);
    return;
  }

  const publisher = HttpPublisher.connect({ endpoint });

  await publisher.publish(topic, {
    topic,
    payload: Buffer.from("hello from typescript"),
    meta: { lang: "ts" }
  });

  console.log("published");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
