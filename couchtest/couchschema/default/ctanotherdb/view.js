// couchtest ctanotherdb testdesigndoc view map
function (doc) {
  emit(doc._id, 1);
}
