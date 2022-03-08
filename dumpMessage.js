const objectSchema = require("./Schema/x2oObjectSchema");

const dumpEventToDatabase = async (uid, record) => {
  try {
    const respo = await objectSchema
      .findOneAndUpdate({ "header.uid": uid }, { ...record }, { upsert: true })
      .catch((e) =>
        console.log("CACHE EXISTS or failed to dump x2o to db.", e)
      );
    console.log("MESSAGE DUMPED TO DB: ");
  } catch (e) {
    console.log("Could not save data to database.", e);
  }
};

module.exports = { dumpEventToDatabase };
