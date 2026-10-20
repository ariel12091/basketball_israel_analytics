import 'server-only';
import {Pool,type PoolClient} from 'pg';
import {Deadline,DeadlineError,BusyError,QUEUE_MS,CONNECT_MS,SQL_MAX_MS} from './deadline';
export {Deadline,DeadlineError,BusyError} from './deadline';
export type Query = <R=Record<string,unknown>>(sql:string,params:unknown[])=>Promise<R[]>;
type Waiting={resolve:()=>void;reject:(e:Error)=>void;deadline:Deadline;timer:ReturnType<typeof setTimeout>};
/** FIFO bounds admitted work before pg's internal queue, so expired work cannot execute later. */
export class SnapshotDatabase {
 private active=0;private waiting:Waiting[]=[];
 constructor(private pool:Pick<Pool,'connect'>,private max=2,private queueMax=20){}
 private async enter(deadline:Deadline){if(deadline.expired())throw new DeadlineError();if(this.active<this.max){this.active++;return;}if(this.waiting.length>=this.queueMax)throw new BusyError();await new Promise<void>((resolve,reject)=>{const entry={} as Waiting;Object.assign(entry,{resolve,reject,deadline,timer:setTimeout(()=>{this.waiting=this.waiting.filter(e=>e!==entry);reject(deadline.expired()?new DeadlineError():new BusyError());},Math.min(QUEUE_MS,deadline.remaining()))});this.waiting.push(entry);});}
 private leave(){this.active--;while(this.waiting.length){const next=this.waiting.shift()!;clearTimeout(next.timer);if(next.deadline.expired()){next.reject(new DeadlineError());continue;}this.active++;next.resolve();break;}}
 async withSnapshot<T>(deadline:Deadline,fn:(q:Query)=>Promise<T>):Promise<T>{
  await this.enter(deadline);let client:PoolClient|undefined,released=false,stopped=false;let overall:ReturnType<typeof setTimeout>|undefined,first:ReturnType<typeof setTimeout>|undefined;
  const destroy=()=>{if(client&&!released){released=true;client.release(true);}};
  try{return await new Promise<T>((resolve,reject)=>{
   const fail=(e:unknown)=>{if(stopped)return;stopped=true;destroy();reject(e);};
   overall=setTimeout(()=>fail(new DeadlineError()),deadline.remaining());
   first=setTimeout(()=>fail(new BusyError()),Math.min(CONNECT_MS,deadline.remaining()));
   void(async()=>{try{
    const checked=await this.pool.connect();client=checked;if(stopped){destroy();return;}
    await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');if(stopped)return;clearTimeout(first);
    let sqlTime=0;
    const q:Query=async<R,>(sql:string,params:unknown[])=>{if(stopped||deadline.expired())throw new DeadlineError();const budget=Math.floor(Math.min(SQL_MAX_MS-sqlTime,deadline.remaining()-500));if(budget<=0)throw new DeadlineError();const began=performance.now();await client!.query("SELECT set_config('statement_timeout', $1, true), set_config('idle_in_transaction_session_timeout','5s',true)",[budget+'ms']);if(stopped)throw new DeadlineError();try{const answer=await client!.query(sql,params);if(stopped)throw new DeadlineError();return answer.rows as R[];}finally{sqlTime+=performance.now()-began;}};
    const result=await fn(q);if(stopped||deadline.expired())throw new DeadlineError();await client.query('COMMIT');if(stopped)return;stopped=true;released=true;client.release();resolve(result);
   }catch(e){fail(e);}})();
  });}finally{clearTimeout(overall);clearTimeout(first);this.leave();}
 }
}
let database:SnapshotDatabase|undefined;
export function withSnapshot<T>(deadline:Deadline,fn:(q:Query)=>Promise<T>):Promise<T>{
 if(!database){const connectionString=process.env.DATABASE_URL_WEB||process.env.DATABASE_URL;if(!connectionString)throw new BusyError();const max=Number(process.env.DB_POOL_MAX??2);if(!Number.isInteger(max)||max<1||max>20)throw new Error('Invalid DB_POOL_MAX');const pool=new Pool({connectionString,max,connectionTimeoutMillis:CONNECT_MS,idleTimeoutMillis:10000,application_name:'ibpl-web'});pool.on('error',()=>{});database=new SnapshotDatabase(pool,max,Number(process.env.DB_QUEUE_MAX??20));}return database.withSnapshot(deadline,fn);
}
