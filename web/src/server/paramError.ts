/** A request parameter the API refuses (400, with the parameter's name). */
export class ParamError extends Error {
  constructor(
    public param: string,
    message: string,
  ) {
    super(message);
  }
}
