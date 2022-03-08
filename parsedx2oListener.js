module.exports = async (recieveParsedx2oToQueue, parsedx2oReciever) => {
  console.log("Awaiting for parsed x2o messages...");
  const myMessageHandler = async (message) => {
    console.log("parsed x2o messages", message);
    if (message.body && Object.keys(message.body).length > 0) {
      await parsedx2oReciever.completeMessage(message);
      await recieveParsedx2oToQueue(message.body);
    }
  };
  const myErrorHandler = async (args) => {
    console.log(
      `Error occurred with ${args.entityPath} within ${args.fullyQualifiedNamespace}: `,
      args.error
    );
  };
  parsedx2oReciever.subscribe({
    processMessage: myMessageHandler,
    processError: myErrorHandler,
  });
};
