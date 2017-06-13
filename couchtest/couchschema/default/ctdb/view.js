// couchtest ctdb testdesigndoc2 view1 map
function (doc) {
  emit(doc._id, 1);
}
