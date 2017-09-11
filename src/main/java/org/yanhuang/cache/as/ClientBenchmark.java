/**
 * 
 */
package org.yanhuang.cache.as;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

/**
 * @author zhyhang
 *
 */
@SuppressWarnings("unchecked")
public class ClientBenchmark {

	private final ClientPolicy cp = initClientPolicy();
	private final Host[] hosts = Host.parseHosts("192.168.152.188:3000", 3000);
	private final MapPolicy mp = new MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE);
	private final String binName = "user_profile";
	private final String ns = "ssd_180d";
	private final String keyPart = "thisisamockkeyforuserprofiler";
	private final String mapKeyPart = "600000";
	private final int totalKey = 100000000;// 总的key个数
	private final int mapSize = 36;// 每个key有36标签
	private final int parallel = 200;
	private final WritePolicy wp = initWritePolicy();

	private ClientPolicy initClientPolicy() {
		ClientPolicy p = new ClientPolicy();
		p.tendInterval = 20000;// 20秒检查一次as cluster
		p.timeout = 2000;
		return p;
	}

	private WritePolicy initWritePolicy() {
		WritePolicy wp = new WritePolicy();
		wp.expiration = 3600 * 24 * 30;
		wp.maxRetries = 0;
		wp.sleepBetweenRetries = 5;
		wp.socketTimeout = 10;
		wp.totalTimeout = 20;
		return wp;
	}

	public AtomicLong[] writeBench() {
		return concurrentRwAs(this::writeOperation);
	}

	public AtomicLong[] readBench() {
		return concurrentRwAs(this::readOperation);
	}

	private Key createKey(long index) {
		return new Key(ns, null, index + keyPart + index);
	}

	private Value createMapKey(int index) {
		return Value.get(mapKeyPart + index);
	}

	private Value createMapValue(long lastInSeconds, double score, String extInfo) {
		long last = lastInSeconds | 0xffffffff00000000L; // 取后4字节的16进制字符
		long lscore = ((long) (score * 100)) | 0xff00000000000000L;
		// 保证固定长度的字符串，让as正常排序
		return Value.get(Long.toHexString(last).substring(8) + Long.toHexString(lscore).substring(6) + "_" + extInfo);
	}

	private static interface AsOperation {
		void doOperation(AerospikeClient client, WritePolicy wp, Key key, Object dealObj);
	}

	private void writeOperation(AerospikeClient client, WritePolicy wp, Key key, Object dealObj) {
		client.operate(wp, key, MapOperation.putItems(mp, binName, (Map<Value, Value>) dealObj));
	}

	private void readOperation(AerospikeClient client, WritePolicy wp, Key key, Object dealObj) {
		client.operate(wp, key, MapOperation.getByRankRange(binName, -20, 20, MapReturnType.KEY_VALUE));
	}

	public AtomicLong[] concurrentRwAs(AsOperation operation) {
		AtomicLong[] stat = new AtomicLong[12];
		Arrays.setAll(stat, i -> new AtomicLong());
		AtomicLong keyIndex = new AtomicLong(0);
		ExecutorService threadPool = Executors.newFixedThreadPool(parallel);
		try (AerospikeClient client = new AerospikeClient(cp, hosts)) {
			for (int i = 0; i < parallel; i++) {
				threadPool.execute(() -> {
					long index = 0;
					while ((index = keyIndex.incrementAndGet()) <= totalKey) {
						Key key = createKey(index);
						Map<Value, Value> inputMap = new HashMap<>();
						for (int j = 0; j < mapSize; j++) {
							inputMap.put(createMapKey(j), createMapValue(System.currentTimeMillis() / 1000,
									ThreadLocalRandom.current().nextDouble(), String.valueOf(j)));
						}
						long ts = System.nanoTime();
						long diff = 0;
						try {
							operation.doOperation(client, wp, key, inputMap);
							diff = System.nanoTime() - ts;
						} catch (Exception e) {
							diff = System.nanoTime() - ts;
							stat[10].incrementAndGet();
							if (ThreadLocalRandom.current().nextDouble() > 0.9) {
								e.printStackTrace();
							}
						}
						stat[11].addAndGet(diff);
						int statIndex = (int) (diff / TimeUnit.MILLISECONDS.toNanos(2));
						if (statIndex > 9) {
							stat[9].incrementAndGet();
						} else {
							stat[statIndex].incrementAndGet();
						}
					}
				});
			}
			threadPool.shutdown();
			threadPool.awaitTermination(24, TimeUnit.HOURS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return stat;

	}

	public static void main(String[] args) {
		if (args.length > 0 && "-r".equals(args[0])) {
			System.out.println("readbench begin: " + Instant.now().atZone(ZoneId.systemDefault()).toString());
			AtomicLong[] readStats = new ClientBenchmark().readBench();
			System.out.println("readbench end: " + Instant.now().atZone(ZoneId.systemDefault()).toString());
			outputStat(readStats);
		} else if (args.length > 0 && "-w".equals(args[0])) {
			System.out.println("writebench begin: " + Instant.now().atZone(ZoneId.systemDefault()).toString());
			AtomicLong[] writeStats = new ClientBenchmark().writeBench();
			System.out.println("writebench end: " + Instant.now().atZone(ZoneId.systemDefault()).toString());
			outputStat(writeStats);
		} else {
			System.out.println("Usage:\n\t-w for write aerospike benchmark\n\t-r for read aerospike benchmark");
		}
	}

	private static void outputStat(AtomicLong[] stats) {
		System.out.println("write stats:");
		long sum = 0;
		for (int i = 0; i < 10; i++) {
			sum += stats[i].get();
		}
		System.out.println("total keys: " + sum);
		System.out.println("totoal time in ms: " + (TimeUnit.NANOSECONDS.toMillis(stats[11].get())));
		System.out.println("average time in ms: " + TimeUnit.NANOSECONDS.toMillis((stats[11].get() / sum)));
		System.out.println("error keys: " + stats[10].get());
		System.out.println("time cost deploy:");
		for (int i = 0; i < 10; i++) {
			System.out.println("\tcount of " + (i * 2) + " ms: " + stats[i].get());
		}
	}
}
