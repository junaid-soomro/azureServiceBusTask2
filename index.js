const x2oListener = require("./x2oListener");
const parsedx2oListener = require("./parsedx2oListener");
const objectCache = require("./Schema/x2oObjectSchema");
const {
  delay,
  ServiceBusClient,
  ServiceBusAdministrationClient,
} = require("@azure/service-bus");
require("dotenv").config();

//--------------UTILITY FUNCTIONS-------------//
const createSubscription = async (
  subscription,
  topicName,
  subscriptionName,
  conString
) => {
  const serviceBusAdministrationClient = new ServiceBusAdministrationClient(
    conString
  );
  subscription = await serviceBusAdministrationClient
    .getSubscription(topicName, subscriptionName)
    .catch((err) => console.log("Subscription does not exist. Creating.."));

  if (!subscription) {
    await serviceBusAdministrationClient
      .createSubscription(topicName, subscriptionName)
      .catch((err) => {
        console.log("Failed to subscribe to topic: ", err);
        process.exit(1);
      });
  }

  return subscription;
};
//--------------------------------------------//

//global variables. of course i could have created a functtion for this but lack of time.

const serviceClient = new ServiceBusClient(process.env.AZURE_CONNECTION_STRING);

const inputQueueServiceClient = new ServiceBusClient(
  process.env.AZURE_INPUTQUEUE_CON_STRING
);
const outputQueueServiceClient = new ServiceBusClient(
  process.env.AZURE_OUTPUT_CON_STRING
);

//an IIFE is the main function.
(async () => {
  if (!(await require("./singletonConnection").getConnection())) {
    await require("./singletonConnection").initConnection();
  }

  const topicName = process.env.TOPIC_NAME;
  const externalTopic = process.env.EXTERNAL_OUTPUT_TOPIC;

  const subscriptionName = "task2";

  //One for managing subscriptions and the other for recieving purposes.

  let subscription,
    outgoingSub = null;

  //first receiever topic
  subscription = await createSubscription(
    subscription,
    topicName,
    subscriptionName,
    process.env.AZURE_CONNECTION_STRING + topicName
  );

  //final receipient topic.
  outgoingSub = await createSubscription(
    outgoingSub,
    externalTopic,
    subscriptionName,
    process.env.AZURE_CONN_EXTERNAL_OUTPUT
  );

  const x2oReciever = serviceClient.createReceiver(topicName, subscriptionName);
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
      return;
    } catch (e) {
      console.log("Sending message failed exiting...");
      process.exit(1);
    }
  };

  //start listening for x2o objects and dump them into the databse.
  x2oListener(sendParsedx2oToQueue, x2oReciever);

  await parsedx2oListener(recieveParsedx2oToQueue, parsedx2oReciever);

  process.on("SIGINT", async () => {
    console.log("TERMINATING JOB.");
    clearInterval(interval);
    await x2oReciever.close();
    await serviceClient.close();
    process.exit(1);
  });

  process.on("SIGTERM", async function () {
    console.log("TERMINATING JOB.");
    clearInterval(interval);
    await x2oReciever.close();
    await serviceClient.close();
    process.exit(1);
  });
  // a smart tweak to make the application behave as a listener because azure javascript library does not provide asynchronous listening for queues and topics.
  setInterval(async () => await delay(3999), 4000);
  await delay(5000);
})();

const recieveParsedx2oToQueue = async (data) => {
  try {
    const cacheData = await objectCache
      .findOne({ "header.uid": data.uid })
      .lean();

    const mergedObj = { ...cacheData, ...data };

    const outgoinServiceClient = new ServiceBusClient(
      process.env.AZURE_CONN_EXTERNAL_OUTPUT
    );

    const sender = await outgoinServiceClient.createSender(
      process.env.EXTERNAL_OUTPUT_TOPIC
    );

    const message = { body: { ...mergedObj } };

    await sender.sendMessages(message);

    await objectCache.findOneAndDelete({ "header.uid": data.uid });

    console.log("CACHE DELETED AND OBJECT SENT TO TOPIC...");
  } catch (e) {
    console.log("Could not store final x2o in mongodb", e);
  }
};
