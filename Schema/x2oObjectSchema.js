var mongoose = require("mongoose");

const schema = new mongoose.Schema(
  {
    header: { uid: { type: String, index: true } },
  },
  { strict: false }
);

const objectCacheSchema = mongoose.model("objectsCache", schema);

module.exports = objectCacheSchema;
