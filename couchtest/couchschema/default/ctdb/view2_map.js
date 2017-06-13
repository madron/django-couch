// couchtest ctdb testdesigndoc1 view2 map
function (doc) {
  emit(doc._id, 1);
}
