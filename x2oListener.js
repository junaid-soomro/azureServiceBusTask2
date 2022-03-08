const dumpToDb = require("./dumpMessage").dumpEventToDatabase;

module.exports = async (sendParsedx2oToQueue, receiver) => {
  //this is where i will store the cache schema.
  let filteredCacheSchema = {};

  console.log("Awaiting MESSAGES...");

  console.log("Listening for x2o objects...");

  const myMessageHandler = async (message) => {
    console.log("MESSAGES", message);
    if (message.body && Object.keys(message.body).length > 0) {
      const {
        header: { uid, orgId },
        object: { objectName, rawBlobKey },
      } = message.body;

      filteredCacheSchema = { uid, orgId, name: objectName, rawBlobKey };
      await dumpToDb(uid, message.body);
      await receiver.completeMessage(message);
      await sendParsedx2oToQueue(filteredCacheSchema);
    }
  };
  const myErrorHandler = async (args) => {
    console.log(
      `Error occurred with ${args.entityPath} within ${args.fullyQualifiedNamespace}: `,
      args.error
    );
  };
  receiver.subscribe({
    processMessage: myMessageHandler,
    processError: myErrorHandler,
  });
};
