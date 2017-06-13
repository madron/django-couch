// couchtest ctdb testdesigndoc1 view1 reduce
function (keys, values, rereduce) {
  if (rereduce) {
    return sum(values);
  } else {
    return values.length;
  }
}
