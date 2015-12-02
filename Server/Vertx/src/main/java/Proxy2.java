import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.TimeZone;

/**
 * Live test version
 * @author zhangmi
 * q1 elb
 * q2-q4 round roubin forward
 * q5 forward to a specific server
 * q6 - sharding
 */

public class Proxy2 extends AbstractVerticle {
	// jdbc client.
	private JDBCJava jdbc;
	// catch
	private String teamId = "QiDeLongDongQiang,642224241148\n";
	private BigInteger X = new BigInteger(
			"8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773");
	
	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	Calendar cal = Calendar.getInstance();
	HashMap<String, Integer> KeyStore1 = new HashMap<String, Integer>();
	private static final int hostnum = 10;
	private static final int q5host = 10;
	private static String[] DNS = new String[hostnum+1];
	private int count = 0;
	
	@Override
	public void start() throws Exception {
		System.out.println("*********** start **************");
		jdbc = new JDBCJava();
		HttpClient client = vertx.createHttpClient(new HttpClientOptions());
		DNS[0] = "ec2-54-84-179-42.compute-1.amazonaws.com";
		DNS[1] = "ec2-54-152-19-185.compute-1.amazonaws.com";
		DNS[2] = "ec2-54-173-184-45.compute-1.amazonaws.com";
		DNS[3] = "ec2-54-172-141-42.compute-1.amazonaws.com";
		DNS[4] = "ec2-54-85-183-235.compute-1.amazonaws.com";
		DNS[5] = "ec2-54-86-72-252.compute-1.amazonaws.com";
		DNS[6] = "ec2-52-90-220-245.compute-1.amazonaws.com";
		DNS[7] = "ec2-54-85-207-18.compute-1.amazonaws.com";
		DNS[8] = "ec2-52-91-14-239.compute-1.amazonaws.com";
		DNS[9] = "ec2-54-172-186-84.compute-1.amazonaws.com";
		
		//q5 host
		DNS[10] = "ec2-52-91-70-44.compute-1.amazonaws.com";

		// connection
		HttpServer server = vertx.createHttpServer();
		server.requestHandler(req -> {
				String uri = req.uri();
				if (uri.indexOf("/q1?") == -1) {
					int dnsNum = getDnsNum(uri, count);
					// dnsNum == 50 means illegal, ignore illegal request
					if (dnsNum != 50) {
							if(dnsNum!=100){
								// dnsNum!=100 means opt-r or opt-a, or other request send it round robin
								HttpClientRequest c_req = client.request(req.method(), 80,
										DNS[dnsNum], req.uri(), c_res -> {
											req.response().setChunked(true);
											req.response().setStatusCode(c_res.statusCode());
											req.response().headers().setAll(c_res.headers());
											c_res.handler(data -> {
												req.response().write(data);
											});
											c_res.endHandler((v) -> req.response().end());
										});
								c_req.setChunked(false);
								c_req.headers().setAll(req.headers());
								req.endHandler((v) -> c_req.end());
								count += 1;
								count = count % hostnum;	
							}else{
								// dnsNum ==100, means opt=s or opt = e, send request to all 
								req.response().putHeader("content-type", "text/html; charset=UTF-8").end(teamId+"0\n");
								for(int newCount =0; newCount<hostnum;newCount++){
									client.getNow(80,DNS[newCount],req.uri(),
		                                    resp -> {
		                                        resp.bodyHandler(body -> {
		                                        }
		                                       );
		                                    });
								}
							}
				}
			} else {	
				// parse and get q1
				String key = getQueryKey(uri);
				String response="";
				if(key.equals("")==false){
					response =  teamId + key.substring(3);
				}
				req.response().putHeader("content-type", "text/html; charset=UTF-8").end(response);
			}
	
		}	).listen(8080);
	}

	

	/**
	 * Get the dns num for q2-q4
	 * 
	 * @param uri
	 * @param count
	 * @return
	 */
	private int getDnsNum(String uri, int count) {
		int dnsNum = count;
		if (uri.indexOf("/q6?") != -1) {
			// q6 judge
			int opts = uri.indexOf("opt=s");
			int opte = uri.indexOf("opt=e");
			int optr = uri.indexOf("opt=r");
			int idIndex = uri.indexOf("tweetid=");
			int andIndex = uri.indexOf("&tag=");
			if (opts == -1 && opte == -1) {
				String tweetid="";
				if(optr==-1){
					tweetid = uri.substring(idIndex + 8, andIndex);
				}else{
					tweetid = uri.substring(idIndex + 8);
				}
				dnsNum = Math.abs(tweetid.hashCode() % hostnum);
			} else {
				// means opts or opte
				dnsNum = 100;
			}
		} else if (uri.indexOf("/q5?") != -1) {
			// q5 request
			dnsNum = q5host;
		}else if(uri.indexOf("/q2?") == -1 && uri.indexOf("/q3?") == -1
				&& uri.indexOf("/q4?") == -1) {
			// means illegal request
			dnsNum = 50;
		} else {
			// request 2,3,4
			dnsNum = count;
		}
		return dnsNum;
	}

	/***************************************************************************
	 * Parse Key and Build Result
	 **************************************************************************/

	/**
	 * parse the request to generate the key in order to query in database
	 * 
	 * @param input
	 *            request string
	 * @return query key
	 */
	public String getQueryKey(String input) {
		return parseQ1(input);
	}

	private String parseQ1(String input) {

		dateFormat.setTimeZone(TimeZone.getTimeZone("PRT"));
		String put = dateFormat.format(cal.getTime()) + "\n";

		int index = input.indexOf("&");
		int keyIndex = input.indexOf("key");
		if (index != -1 && keyIndex != -1) {
			String message = input.substring(index + 9);
			int l = message.length();
			int n = (int) Math.sqrt(l);

			String xy = input.substring(keyIndex + 4, index);
			int Z;
			if (KeyStore1.containsKey(xy)) {
				Z = KeyStore1.get(xy);
			} else {
				BigInteger XY = new BigInteger(xy);
				String Y = XY.divide(X).toString();
				Z = Integer.valueOf(Y.substring(Y.length() - 2)) % 25 + 1;
				KeyStore1.put(xy, Z);
			}
			message = getText(message, n);
			message = moveBit(message, Z);
			return "q1," + put + message + "\n";
		} else {
			return "";
		}

	}

	/********************************************************************
	 * Q1 Helper Function
	 ********************************************************************/

	/**
	 * Return the String move key
	 * 
	 * @param s
	 *            - String need to move bit
	 * @param key
	 *            - number of move bit
	 * @return String - return the original String
	 */
	public String moveBit(String s, int key) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < s.length(); i++) {
			int ascii = (int) s.charAt(i) - key;
			ascii = (ascii < 65) ? ascii + 26 : ascii;
			builder.append((char) ascii);
		}
		return builder.toString();
	}

	/**
	 * Return the caesarify from diagonalize
	 * 
	 * @param text
	 *            - diagonalized text
	 * @param n
	 *            - square size
	 * @return String - return the original string
	 */
	public String getText(String text, int n) {
		StringBuilder builder = new StringBuilder();
		int i, k, begin;
		char[] textChar = text.toCharArray();
		for (int sum = 0; sum <= (n - 1) * 2; sum++) {
			begin = (sum > n - 1) ? sum - n + 1 : 0;
			for (i = begin; i <= sum && i < n; i++) {
				k = i * n + sum - i;
				builder.append(textChar[k]);
			}
		}
		return builder.toString();
	}

	public static UserCountList initializeQ5(String filename) {
		UserCountList list = new UserCountList();

		// round 2: read score list
		BufferedReader reader = null;
		long uid;
		int sum;
		int count = 0;
		String line;
		try {
			reader = new BufferedReader(new FileReader(new File(filename)));

			while ((line = reader.readLine()) != null) {
				count++;
				if (count % 5000000 == 0) {
					System.out.print(count / 5000000 + " ");
				}
				String[] seg = line.split("\t");
				if (seg.length != 3)
					continue;
				uid = Long.parseLong(seg[0]);
				sum = Integer.parseInt(seg[1]);
				list.add(uid, sum);
			}
			reader.close();
		} catch (Exception e) {
			System.out.print("Loading q5 file failed.");
		}

		System.out.println("\nQ5: " + count + " loaded! (should be 53767998)");
		return list;
	}
}

class UserCountList {
	// maxID 2594997268
	// minID 12
	// -2147483648 ~ 2147483647
	private int[] id = null;
	private int[] count = null;
	private int size = 0;
	private static final int TOTAL = 53767998 + 1;
	private static final long MINID = 12;
	private static final long MAXID = 2594997268L;
	private final int UID_SHIFT = 1000000000;

	public UserCountList() {
		id = new int[TOTAL];
		count = new int[TOTAL];
		id[0] = 0;
		count[0] = 0;
		size++;
	}

	public void add(long uid, int sum) {
		int newid = (int) (uid - UID_SHIFT);
		id[size] = newid;
		count[size] = sum;
		size++;
	}

	private int binSearchUidLeft(int[] array, int target, int beginPos,
			int endPos) {
		// [...)
		while (1 < endPos - beginPos) {
			int mid = (beginPos + endPos) / 2;
			if (target < array[mid]) {
				endPos = mid;
			} else {
				beginPos = mid;
			}
		}
		if (target == array[beginPos]) {
			return beginPos - 1;
		} else {
			return beginPos;
		}

	}

	private int binSearchUid(int[] array, int target, int beginPos, int endPos) {
		// [...)
		while (1 < endPos - beginPos) {
			int mid = (beginPos + endPos) / 2;
			if (target < array[mid]) {
				endPos = mid;
			} else {
				beginPos = mid;
			}
		}
		return beginPos;
	}

	public int getCount(String q5str) {
		String[] seg = q5str.split(",");

		long left = Long.parseLong(seg[1]);
		long right = Long.parseLong(seg[2]);

		return search(left, right);
	}

	private int search(long left, long right) {
		if (left < MINID) {
			left = MINID;
		}
		if (right > MAXID) {
			right = MAXID;
		}
		int leftpos = binSearchUidLeft(id, (int) (left - UID_SHIFT), 1, TOTAL);
		int rightpos = binSearchUid(id, (int) (right - UID_SHIFT), 1, TOTAL);
		return count[rightpos] - count[leftpos];
	}
}
