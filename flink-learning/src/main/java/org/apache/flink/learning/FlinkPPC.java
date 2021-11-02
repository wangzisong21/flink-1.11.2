package org.apache.flink.learning;

import akka.actor.ActorSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceConfiguration;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author: wangzisong
 * @date: 2021/11/1
 */
public class FlinkPPC {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // -------------------------------- 服务端启动 --------------------------------
        // 1.启动PRC服务
        ActorSystem defaultActorSystem = AkkaUtils.createDefaultActorSystem();
        AkkaRpcService akkaRpcService = new AkkaRpcService(defaultActorSystem, AkkaRpcServiceConfiguration.defaultConfiguration());

        // 2.创建RpcEndPoint实例，启动RPC服务
        GetNowRpcEndpoint getNowRpcEndpoint = new GetNowRpcEndpoint(akkaRpcService);
        getNowRpcEndpoint.start();



        // -------------------------------- 客户端调用 --------------------------------
        // 3.通过selfGateway调用RPC服务，本地调用
        GetNowGateway selfGateway = getNowRpcEndpoint.getSelfGateway(GetNowGateway.class);
        String result1 = selfGateway.getNow();
        System.out.println(result1);

        // 4.通过RpcEndPoint地址获取代理，远程调用
        GetNowGateway getNowGateway = akkaRpcService.connect(getNowRpcEndpoint.getAddress(), GetNowGateway.class).get();
        String result2 = getNowGateway.getNow();
        System.out.println(result2);

    }


    /**
     * 定义一个通信协议
     */
    public interface GetNowGateway extends RpcGateway {
        String getNow();
    }

    /**
     * 定义了一个功能组件
     */
    static class GetNowRpcEndpoint extends RpcEndpoint implements GetNowGateway{

        protected GetNowRpcEndpoint(RpcService rpcService, String endpointId) {
            super(rpcService, endpointId);
        }

        protected GetNowRpcEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public String getNow() {
            return LocalDateTime.now().toString();
        }

        @Override
        protected void onStart() throws Exception {
            System.out.println("GetNowRpcEndpoint Start");
            super.onStart();
        }

        @Override
        protected CompletableFuture<Void> onStop() {
            System.out.println("GetNowRpcEndpoint Stop");
            return super.onStop();
        }
    }


}


