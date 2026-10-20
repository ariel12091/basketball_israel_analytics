export const QUEUE_MS=500,CONNECT_MS=500,SQL_MAX_MS=3000;
export class DeadlineError extends Error {constructor(){super('Request deadline exceeded');this.name='DeadlineError';}}
export class BusyError extends Error {constructor(){super('Service busy; retry shortly');this.name='BusyError';}}
export class Deadline {private readonly end:number;constructor(totalMs=5000){this.end=performance.now()+totalMs;}remaining(){return Math.max(0,this.end-performance.now());}expired(){return this.remaining()<=0;}}
