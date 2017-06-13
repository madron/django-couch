// couchtest ctdb testdesigndoc1 testview1 map
function (doc) {
  emit(doc._id, 1);
}
