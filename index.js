const x2oListener = require("./x2oListener");
const parsedx2oListener = require("./parsedx2oListener");
const objectCache = require("./Schema/x2oObjectSchema");
const { delay, ServiceBusClient } = require("@azure/service-bus");
require("dotenv").config();

//global variables. .

const serviceInitialTopicClient = new ServiceBusClient(
  process.env.AZURE_CONNECTION_STRING
);

const inputQueueServiceClient = new ServiceBusClient(
  process.env.AZURE_INPUTQUEUE_CON_STRING
);
const outputQueueServiceClient = new ServiceBusClient(
  process.env.AZURE_OUTPUT_CON_STRING
);

const serviceFinalTopicClient = new ServiceBusClient(
  process.env.AZURE_CONN_EXTERNAL_OUTPUT
);

//an IIFE is the main function.
(async () => {
  if (!(await require("./singletonConnection").getConnection())) {
    await require("./singletonConnection").initConnection();
  }

  const topicName = process.env.TOPIC_NAME;

  //subscription will already be there.
  const subscriptionName = "task2";

  const x2oReciever = serviceInitialTopicClient.createReceiver(
    topicName,
    subscriptionName
  );

  const parsedx2oReciever = outputQueueServiceClient.createReceiver(
    process.env.INTERNAL_OUTPUT_QUEUE
  );

  //this is a callback function once the message has been recieved.
  const sendParsedx2oToQueue = async (data) => {
    try {
      const sender = inputQueueServiceClient.createSender(
        process.env.INTERNAL_INPUT_QUEUE
      );

      if (!sender) process.exit(1);

      await sender.sendMessages([{ body: data }]);
      console.log("MESSAGE SENT TO INPUT QUEUE: ", data);
      return;
    } catch (e) {
      console.log("Sending message failed exiting...");
      process.exit(1);
    }
  };

  //start listening for x2o objects and dump them into the databse.
  x2oListener(sendParsedx2oToQueue, x2oReciever);

  parsedx2oListener(recieveParsedx2oToQueue, parsedx2oReciever);

  // a smart tweak to make the application behave as a listener because azure javascript library does not provide asynchronous listening for queues and topics.
  let interval = setInterval(async () => await delay(3999), 4000);
  await delay(5000);
  const cleanUp = async () => {
    console.log("TERMINATING JOB.");
    clearInterval(interval);
    await x2oReciever.close();
    await serviceInitialTopicClient.close();
    process.exit(1);
  };

  process.on("SIGINT", cleanUp);
  process.on("SIGQUIT", cleanUp);
  process.on("SIGTERM", cleanUp);
})();

const recieveParsedx2oToQueue = async (data) => {
  try {
    const cacheData = await objectCache
      .findOne({ "header.uid": data.uid })
      .lean();

    const mergedObj = { ...cacheData, ...data };

    const sender = await serviceFinalTopicClient.createSender(
      process.env.EXTERNAL_OUTPUT_TOPIC
    );

    const message = { body: { ...mergedObj } };

    await sender.sendMessages(message);

    await objectCache.findOneAndDelete({ "header.uid": data.uid });
    await sender.close();
    console.log("CACHE DELETED AND OBJECT SENT TO TOPIC...");
  } catch (e) {
    console.log("Could not store final x2o in mongodb", e);
  }
};
