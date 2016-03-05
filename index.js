//scheduler global variables
var CUR_UTHREAD_TID = 0;

function u_thread(ctx, initState, initBuffer) {
  CUR_UTHREAD_TID++;
  const tlb = initBuffer || [];
  const tls = initState || new Map();
  tls.set("u_thread_status", "READY");

  this.tid = CUR_UTHREAD_TID;
  this.ctx = ctx(tls); // context is initialized with a reference to tls
  this.set = (k, v) => tls.set(k, v); // expose a function to set tls vars
  this.status = () => tls.get("u_thread_status"); // get status
  // send this uthread a message
  this.send = (message) => tlb.unshift(message);
  // if we have any pending messages in our buffer, process the next one
  this.run = () =>
    (tlb.length > 0)
      ? this.ctx.next(tlb.pop())
      : this.ctx.next();
};

function Mechanisms(scheduler, ...defaults) {
  const mechanisms = new Map();
  this.add = (name, mechanism) => mechanisms.set(name, mechanism(scheduler));
  this.call = (mechanism, ...args) => mechanisms.get(mechanism)(...args);

  if (defaults && defaults.length > 0) {
    defaults.forEach(([name, mechanism]) => this.add(name, mechanism));
  }
};

function SystemCalls(scheduler, ...defaults) {
  const syscalls = new Map();
  this.add = (name, syscall) => syscalls.set(name, syscall(scheduler));
  this.call = (syscall, ...args) => syscalls.get(syscall)(...args);

  if (defaults && defaults.length > 0) {
    defaults.forEach(([name, syscall]) => this.add(name, syscall));
  }
}


// create a new uthread
function uthread_create(scheduler) {
  return function(ctx) {
    const thread = new u_thread(ctx);
    const tid = thread.tid;
    scheduler.threads.set(tid, thread);
    scheduler.schedule(thread);
    return tid;
  };
};

// parks a uthread to wait on another threads exit
function uthread_wait(scheduler) {
  return function(toWait, waitOn) {
    if (scheduler.threads.get(waitOn)) {
      const waiting = scheduler.waiting;
      scheduler.threads.get(toWait).set("u_thread_status", "BLOCKED");
      (waiting.get(waitOn))
        ? waiting.get(waitOn).unshift(toWait)
        : waiting.set(waitOn, [ toWait ]);
      return true;
    } else {
      return false;
    }
  }
};

// deletes a uthread and schedules all waiting uthreads
function uthread_exit(scheduler) {
  return function(tid, error) {
    const waiters = scheduler.waiting.get(tid);
    // delete task from taskMap
    scheduler.threads.delete(tid);
    if (waiters && waiters.length > 0) {
      // schedule all tasks waiting on tid, need to find a way to indicate that
      // these tasks were scheduled because tid exited
      for (var i = 0; i < waiters.length; i++) {
        scheduler.schedule(scheduler.threads.get(waiters.pop()));
      }
      scheduler.waiting.delete(tid);
    }
    if (error) {
      console.log("Task %s terminated with error: %s", tid, error);
    } else {
      console.log("Task %s terminated cleanly", tid);
    }
    return true;
  }
};

// retrieve the status of a uthread
function uthread_status(scheduler) {
  return function(tid) {
    const thread = scheduler.threads.get(tid);
    if (thread) {
      return thread.status();
    }
  }
};

function UdpSocket(message_handler, error_handler, close_handler, listening_handler) {
  const handlers = Object.create(null);
  handlers.message = message_handler;
  handlers.error = error_handler;
  handlers.close = close_handler;
  handlers.listening = listening_handler;
  return (uthread) => (scheduler, bindOpts) => {
    const newHandlers = Object.keys(handlers).reduce((obj, key) => {
      handlers[key] = handlers[key](uthread)(scheduler, bindOpts);
      return obj;
    }, handlers);
    return newHandlers;
  };
};

const sockets = Object.create(null);
sockets.udp = ([type, reuse]) => require("dgram").createSocket(type || "udp4", reuse || "false");

function socket(scheduler) {
  try {
  return function(rw, kind, socketOpts, bindOpts, spec) {
    const socket = sockets[kind](socketOpts);
    if (rw === "r") {
      const handlers = spec(scheduler, bindOpts);
      Object.keys(handlers).reduce((obj, key) => {
        obj.on(key, handlers[key]);
        return obj;
      }, socket).bind(bindOpts, () => {
        scheduler.sockets.set(bindOpts.port, socket);
        socket.unref();
      });
      return socket;
    } else {
      return socket;
    }
  }
} catch(e) {
  throw new Error(e);
}
}

function suspend(scheduler) {
  return function(tid) {
    console.log("suspended");
  }
}

const default_mechanisms = [
  ["create", uthread_create],
  ["exit", uthread_exit],
  ["wait", uthread_wait],
  ["status", uthread_status],
  ["socket", socket],
  ["suspend", suspend]
];

function Scheduler(mechanisms, syscalls) {
  this.ready = [];
  this.sockets = new Map();
  this.threads = new Map();
  this.waiting = new Map();
  //this.sls = new Map();

  this.mechanisms = new Mechanisms(this, ...default_mechanisms.concat(mechanisms || []));
  this.syscalls = new SystemCalls(this, ...syscalls || []);

  this.register = (type, fn, name) => {
    switch(type) {
      case "mechanism":
        this.mechanisms.add(name || fn.name, fn);
        break;
      case "syscall":
        this.syscalls.add(name || fn.name, fn);
        break;
      case "uthread":
        this.mechanisms.call("create", fn);
        break;
      default:
        throw new Error("No such type as %s to be registered.", type);
        break;
    }
    return true;
  };

  this.schedule = (uthread) => {
    uthread.set("u_thread_status", "READY");
    this.ready.unshift(uthread);
    return true;
  }

  const dCall = {
    type: "downcall",
    init: function(name, args) {
      this.name = name;
      this.args = args;
      return this;
    }
  };

  this.downcall = (name, ...args) => (Object.create(dCall).init(name, args));

  this.start = () => {
    this.loop();
  }
  // main scheduler loop, pulls tasks off ready queue and runs them to
  // next yield where it then reschedules them.
  this.loop = () => {
    if (this.threads.size > 0 && this.ready.length > 0) {
      const uthread = this.ready.pop();
      uthread.set("u_thread_status", "RUNNING");
      try {
        const { value, done } = uthread.run();
        if (done) {
          this.mechanisms.call("exit", uthread.tid);
          setImmediate(this.loop); // check timers, check i/o, run fn
          return;
        }
        /*
         * If result is a syscall, do some setup and run
         * syscall on behalf of the task. We may want to
         * make this asynchronous: schedule the syscall to
         * be run at a different time, park the task, create
         * callback to reschedule the task when the syscall runs.
         */

        if (dCall.isPrototypeOf(value)) {
          const { name, args } = value;
          this.syscalls.call(name, uthread, ...args);
          setImmediate(this.loop);
          return;
        }
      } catch( e ) {
        this.mechanisms.call("exit", uthread.tid, e);
        setImmediate(this.loop);
        return;
      }
      this.schedule(uthread);
      setImmediate(this.loop);
      return;
    } else {
      setImmediate(this.loop);
      return;
    }
  };
}

// TESTS REALLY ----------------------------------------------------------------

// need to find better api for ad hoc registering of syscalls and mechanisms, if
// any. Need to find better more comprehensive i/o api. Need to find a clearer way
// of expressing interfaces and what a client scheduler needs to implement. Break this
// into a couple modules. Need to have a default process / uthread api. Need to have
// better naming conventions. This is a good first draft.

const myScheduler = new Scheduler();
const { start, register, downcall } = myScheduler;

register("syscall", (scheduler) => (uthread, context) => {
  const tid = scheduler.mechanisms.call("create", context);
  uthread.send(tid);
  scheduler.schedule(uthread);
}, "create");

register("syscall", (scheduler) => (uthread) => {
  const tid = uthread.tid;
  uthread.send(tid);
  scheduler.schedule(uthread);
}, "tid");

register("syscall", (scheduler) => (uthread, tid) => {
  const waiting = scheduler.mechanisms.call("wait", uthread.tid, tid);
  uthread.send(waiting);
}, "wait");

register("syscall", (scheduler) => (uthread, tid) => {
  const status = scheduler.mechanisms.call("status", tid);
  uthread.send(status);
  scheduler.schedule(uthread);
}, "status");

register("syscall", (scheduler) => (uthread, rw, kind, socketOpts, bindOpts, spec) => {
  const socket = scheduler.mechanisms.call("socket", rw, kind, socketOpts, bindOpts, spec(uthread));
  uthread.send(socket);
  scheduler.schedule(uthread);
}, "socket");

register("syscall", (scheduler) => (uthread) => {
  scheduler.mechanisms.call("suspend", uthread.tid);
}, "suspend");

const err = (uthread) => (scheduler) => (error) => {
  uthread.send({ type: "error", error });
  scheduler.schedule(uthread);
};

const message = (uthread) => (scheduler) => (msg, rinfo) => {
  uthread.send({ type: "message", msg, rinfo });
  scheduler.schedule(uthread);
};

const listening = (uthread) => (scheduler, bindOpts) => () => {
  const { address, port } = bindOpts;
  console.log("Server listening at %s:%s", address, port);
};

const close = (uthread) => (scheduler) => () => {
  uthread.send({ type: "closed", status: true });
  scheduler.schedule(uthread);
};

// message, error, close, listening
// handler :: (uthread) -> (scheduler, bindOpts) -> (..args) -> ?
const rw = "r";
const kind = "udp";
const socketOpts = ["udp4", false];

const bindOpts1 = {
  port: 44444,
  address: "localhost"
};
const bindOpts2 = {
  port: 44445,
};

const mySocketSpec1 = UdpSocket(message, err, close, listening);
const mySocketSpec2 = UdpSocket(message, err, close, listening);

register("uthread", function*() {
  const mySocket = yield downcall("socket", rw, kind, socketOpts, bindOpts1, mySocketSpec1);
  while(true) {
    var message = yield downcall("suspend");
    if (message && message.type) {
      switch(message.type) {
        case "error":
          console.log("Error: ", message.error);
          mySocket.close();
          break;
        case "message":
          console.log("Received %s from %s:%s", message.msg, message.rinfo.address, message.rinfo.port);
          break;
        case "closed":
          console.log((message.status) ? "CLOSED SOCKET" : "FAILED TO CLOSE SOCKET");
          break;
        default:
          console.log("I don't recognize messages of type: %s.", message.type);
          break;
      }
    }
  }
});

register("uthread", function*() {
  const mySocket = yield downcall("socket", rw, kind, socketOpts, bindOpts2, mySocketSpec2);
  while(true) {
    var message = yield downcall("suspend");
    if (message && message.type) {
      switch(message.type) {
        case "error":
          console.log("Error: ", message.error);
          mySocket.close();
          break;
        case "message":
          console.log("Received %s from %s:%s", message.msg, message.rinfo.address, message.rinfo.port);
          break;
        case "closed":
          console.log((message.status) ? "CLOSED SOCKET" : "FAILED TO CLOSE SOCKET");
          break;
        default:
          console.log("I don't recognize messages of type: %s.", message.type);
          break;
      }
    }
  }
});


//register("uthread", function*() {
//  var tid = yield downcall("tid");
//
//  var childTid = yield downcall("create", function*() {
//    var status = "UNKOWN";
//    var x = 5;
//    while(x > 0) {
//      console.log("My name is jed, and 1s status is %s", status);
//      x--;
//      status = yield downcall("status", 1);
//    }
//  });
//
//
//  console.log("MY CHILD'S TID IS: ", childTid);
//  console.log("My TID IS: ", tid);
//
//  var waited = yield downcall("wait", childTid);
//
//  console.log((waited) ? "I waited" : "I didn't wait");
//});
start();
