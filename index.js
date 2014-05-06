var zonar = require("zonar");
var helper = require("service.helper");
var zmq = require("zmq");

var externalPub = 'tcp://*:8989';
var internalSub = 'tcp://*:5556';
var hwm = 1000;
var verbose = 0;

var subSock = zmq.socket('xsub');
subSock.identity = 'subscriber' + process.pid;
subSock.bindSync(internalSub);

var pubSock = zmq.socket('xpub');
pubSock.identity = 'publisher' + process.pid;
pubSock.setsockopt(zmq.ZMQ_SNDHWM, hwm); //  Set high water mark for outbound messages

// By default xpub only signals new subscriptions
// Settings it to verbose = 1 , will signal on every new subscribe
pubSock.setsockopt(zmq.ZMQ_XPUB_VERBOSE, verbose);
pubSock.bindSync(externalPub);

// When we receive data on subSock , it means someone is publishing
subSock.on('message', function(data) {
    // We just relay it to the pubSock, so subscribers can receive it
    pubSock.send(data);
});

// When Pubsock receives a message , it's subscribe requests
pubSock.on('message', function(data, bla) {
    // The data is a slow Buffer
    // The first byte is the subscribe (1) /unsubscribe flag (0)
    var type = data[0]===0 ? 'unsubscribe' : 'subscribe';
    // The channel name is the rest of the buffer
    var channel = data.slice(1).toString();
    console.log(type + ':' + channel);
    // We send it to subSock, so it knows to what channels to listen to
    subSock.send(data);
});


function proxyPublish(addr){
    var pub = zmq.socket('pub');
    var sub = zmq.socket('sub');

    pub.connect("tcp://127.0.0.1:8989");

    sub.connect(addr);
    sub.subscribe("");

    sub.on("message", function(msg){
        console.log("got msg : " + msg.toString("utf8"));
        pub.send(msg);
    });
}

function simpleTestSub(){
    proxyPublish("tcp://172.16.135.109:6666");
}

//simpleTestSub();

function proxyZonar(){

    var forwarders = {};

    var z = zonar.create({ net : '24hr', name: 'pubsub_forwarder', verbose : true});

    console.log("listening for connections...");
    z.on("found", function(node){

        // do we have any payload at all?
        if (typeof node.payload != "object"){
            console.log("no payload for node : " + node.name);
            return;
        }

        for(var key in node.payload){
            var service = node.payload[key];
            if( service.type == 'pub' && service.port) {
                console.log("found pub service : ", service);
                var full_name = node.name + "." + key;
                var addr = helper.getServiceAddress(z, full_name);
                handler.add(addr, full_name);

                z.on("dropped." + node.name, function(){
                    handler.remove(full_name);
                });


            }
        }
    });

    z.start();

    helper.handleInterrupt(z);
}

var handler = (function(){

    var list = {};

    return {
        add : function (addr, full_name){

            var pub = zmq.socket('pub');
            var sub = zmq.socket('sub');

            pub.connect("tcp://127.0.0.1:5556");

            sub.connect(addr);
            console.log("connecting to addr : " + addr);
            sub.subscribe("");

            sub.on("message", function(msg){
                console.log("got internal message : " + msg.toString());

                pub.send(msg);
            });

            list[full_name] = {
                pub : pub,
                sub : sub
            };
            console.log("current list : ", list);
        },
        remove : function(full_name){
            var pair = list[full_name];
            if (pair){
                console.log("closing down publisher %s", full_name);
                delete list[full_name];
                pair.sub.close();
                pair.pub.close();
                console.log("current list : ", list);
            }
        }
    };
})();

proxyZonar();

