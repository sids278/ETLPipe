// An Error the app's error handler turns into this HTTP status.
export function httpError(status, message) {
  const err = new Error(message);
  err.status = status;
  return err;
}
