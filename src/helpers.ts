
export function showBytes(bytes) {
  let num = bytes;
  let unit = 'B';
  if(num > 1024) {
    num /= 1024;
    unit = 'KB';
  }
  if(num > 1024) {
    num /= 1024;
    unit = 'MB';
  }
  if(num > 1024) {
    num /= 1024;
    unit = 'GB';
  }
  return `${num.toFixed(1)}${unit}`;
}
