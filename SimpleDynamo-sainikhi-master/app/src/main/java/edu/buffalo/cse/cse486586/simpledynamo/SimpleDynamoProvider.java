
package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

	HelperDb msgHelperDb;
	String present_port;
	private String my_hashval = "";
	private ArrayList<String> node_hashes;
	private ArrayList<String> chord_ring;
	String predecessor_hashval = "";
	String successor_hashval = "";
	int predecessor_port = 0;
	int successor_port = 0;
	private Semaphore rec_lock;
	HashMap<Integer,String> operations;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		String dbTable = "msgTable";
		String delete_key = selection;
		String delete_key_hashval = null;
		try{
			delete_key_hashval = genHash(delete_key);
		}catch(NoSuchAlgorithmException e){
			e.printStackTrace();
		}
		Log.v(TAG, "Delete request for : "+delete_key);		if (selection.equals("@")) {
			msgHelperDb.getWritableDatabase().delete(dbTable,null, null);
			return 1;
		}else if(selection.equals("*")){
			Request delete_req = new Request();
			delete_req.setAction("DELETE");
			delete_req.setPresent_port(present_port);
			delete_req.setNeighbors("*");
			try{
				String delete_response = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, delete_req, successor_port * 2).get();
				if(("FAILED").equals(delete_response)){
					int i = chord_ring.indexOf(successor_hashval);
					if(i==4)
						i=0;
					else
						i= i+1;
					int new_suc = 5554 + 2*(node_hashes.indexOf(chord_ring.get(i)));
					delete_response = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, delete_req, new_suc * 2).get();

				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}else{
			Request delete_key_req = new Request();
			delete_key_req.setAction("DELETE");
			delete_key_req.setPresent_port(present_port);
			delete_key_req.setNeighbors(selection);
			String delete_coordinator_hashval = "";
			int i=0;
			while(i<=4){
				if(i==4){
					delete_coordinator_hashval = chord_ring.get(0);
					break;
				}
				if(delete_key_hashval.compareTo(chord_ring.get(i)) > 0 && delete_key_hashval.compareTo(chord_ring.get(i+1)) <= 0){
					delete_coordinator_hashval = chord_ring.get(i+1);
					break;
				}
				i++;
			}
			int delete_coordinator_port = 5554 + 2*(node_hashes.indexOf(delete_coordinator_hashval));
			Log.v(TAG, "delete Coord port is: "+ delete_coordinator_port);
			try{
				if(delete_coordinator_port == Integer.parseInt(present_port)){
					Log.v(TAG,"HIT delete at coord point");
					int rows = msgHelperDb.getWritableDatabase().delete("msgTable","key =?",new String[]{selection});
					delete_key_req.setAction("DELETE_FIRST_REPLICA");
					String delete_status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, delete_key_req, successor_port * 2).get();
					if(("SUCCESS").equals(delete_status)){
						return 1;
					}
					if(("FAILED").equals(delete_status)){
						delete_key_req.setAction("DELETE_SECOND_REPLICA");
						int index = chord_ring.indexOf(successor_hashval);
						if(index==4)
							index=0;
						else
							index= index+1;
						int new_suc = 5554 + 2*(node_hashes.indexOf(chord_ring.get(index)));
						delete_status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, delete_key_req, new_suc * 2).get();
						//if(("SUCCESS").equals(delete_status)){
							return 1;
						//}
					}
					//Log.v(TAG,"FINAL INSERT STATUS(same coord) for key: "+key_value[1]+ " is: "+ insert_status);
				}else {
					Log.v(TAG,"Inside delete at NON-Coord point");
					String delete_key_response = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, delete_key_req, delete_coordinator_port * 2).get();
					if (("FAILED").equals(delete_key_response)) {
						delete_key_req.setPresent_port(Integer.toString(delete_coordinator_port));
						delete_key_req.setAction("DELETE_FIRST_REPLICA");
						int ind = chord_ring.indexOf(delete_coordinator_hashval);
						if (ind == 4)
							ind = 0;
						else
							ind = ind + 1;
						int new_coord = 5554 + 2 * (node_hashes.indexOf(chord_ring.get(ind)));
						String insert_status2 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, delete_key_req, new_coord * 2).get();
						if (("FAILED").equals(insert_status2)) {
							delete_key_req.setAction("DELETE");
							delete_key_req.setPresent_port(present_port);
							delete_key_response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, delete_key_req, delete_coordinator_port * 2).get();
						}
						return 1;
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		return 1;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String dbTable = "msgTable";
		String key_hashval= "";
		String key = values.getAsString("key");
		try{
			key_hashval = genHash(key);
		}catch(NoSuchAlgorithmException e){
			e.printStackTrace();
		}
		int i = 0;
		String coordinator_hashval = "";
		while(i<=4){
			if(i==4){
				coordinator_hashval = chord_ring.get(0);
				break;
			}
			if(key_hashval.compareTo(chord_ring.get(i)) > 0 && key_hashval.compareTo(chord_ring.get(i+1)) <= 0){
				coordinator_hashval = chord_ring.get(i+1);
				break;
			}
			i++;
		}
		int coordinator_port = 5554 + 2*(node_hashes.indexOf(coordinator_hashval));
		Log.v(TAG, "Coord port is: "+ coordinator_port);
		String content = "key#" + key + "#value#" + values.getAsString("value");
		Request insert_msg = new Request();
		insert_msg.setAction("INSERT");
		insert_msg.setPresent_port(present_port);
		insert_msg.setNeighbors(content);
		String[] key_value = content.split("#");
		ContentValues content_values = new ContentValues();
		content_values.put("key", key_value[1]);
		content_values.put("value", key_value[3]);
		try {
			if(coordinator_port == Integer.parseInt(present_port)){
				msgHelperDb.getWritableDatabase().insertWithOnConflict("msgTable", null, content_values, SQLiteDatabase.CONFLICT_REPLACE);
				insert_msg.setAction("FIRST_REPLICA");
				String insert_status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, successor_port * 2).get();
				if(("SUCCESS").equals(insert_status)){
					return uri;
				}
				if(("FAILED").equals(insert_status)){
					int ind = chord_ring.indexOf(successor_hashval);
					if(ind==4)
						ind=0;
					else
						ind= ind+1;
					int new_suc = 5554 + 2*(node_hashes.indexOf(chord_ring.get(ind)));
					insert_msg.setAction("SECOND_REPLICA");
					insert_status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, new_suc * 2).get();
					//if(("SUCCESS").equals(insert_status)){
						return uri;
					//}
				}
			Log.v(TAG,"FINAL INSERT STATUS(same coord) for key: "+key_value[1]+ " is: "+ insert_status);
			}else {
				String insert_status1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, coordinator_port * 2).get();

				if (("SUCCESS").equals(insert_status1)) {
					return uri;
				} else if (("FAILED").equals(insert_status1)) {
					insert_msg.setPresent_port(Integer.toString(coordinator_port));
					insert_msg.setAction("FIRST_REPLICA");
					int ind = chord_ring.indexOf(coordinator_hashval);
					if (ind == 4)
						ind = 0;
					else
						ind = ind + 1;
					int new_coord = 5554 + 2 * (node_hashes.indexOf(chord_ring.get(ind)));
					insert_status1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, new_coord * 2).get();
					if (("FAILED").equals(insert_status1)) {
						insert_msg.setAction("INSERT");
						insert_msg.setPresent_port(present_port);
						insert_status1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, coordinator_port * 2).get();
						return uri;
					} else if (("SUCCESS").equals(insert_status1)) {
						return uri;
					}
				}
				Log.v(TAG,"FINAL INSERT STATUS for key: "+key_value[1]+ " is: "+ insert_status1);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return uri;
	}

	@Override
	public boolean onCreate() {
		msgHelperDb = new HelperDb(getContext());
		node_hashes = new ArrayList<String>();
		chord_ring = new ArrayList<String>();
		rec_lock = new Semaphore(1,true);
		operations = new HashMap<Integer, String>();

		for(int p = 5554; p <= 5562; p=p+2){
			try{
				node_hashes.add(genHash(Integer.toString(p)));
				chord_ring.add(genHash(Integer.toString(p)));
			}catch(NoSuchAlgorithmException e){
				e.printStackTrace();
			}
		}
		Collections.sort(chord_ring);
		Log.v(TAG, "Chord ring sequence is:");
		for(int i=0; i< 5;i++){
			int p = 5554 + 2*(node_hashes.indexOf(chord_ring.get(i)));
			Log.v(TAG,"avd at i: "+ i + Integer.toString(p));
		}
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(getContext().TELEPHONY_SERVICE);
		String port = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		present_port = port;
		try{
			my_hashval = genHash(port);
		}catch(Exception e){
			e.printStackTrace();
		}
		// if my port if first in the chord ring, set predecessor and successor
		if(chord_ring.indexOf(my_hashval) == 0){
			predecessor_hashval = chord_ring.get(4);
			successor_hashval = chord_ring.get(1);
		}else if(chord_ring.indexOf(my_hashval) == 4){
			predecessor_hashval = chord_ring.get(3);
			successor_hashval = chord_ring.get(0);
		}else{
			predecessor_hashval = chord_ring.get(chord_ring.indexOf(my_hashval) - 1);
			successor_hashval = chord_ring.get(chord_ring.indexOf(my_hashval) + 1);
		}
		predecessor_port = 5554 + 2*(node_hashes.indexOf(predecessor_hashval));
		successor_port = 5554 + 2*(node_hashes.indexOf(successor_hashval));

		try{
			rec_lock.acquire();
			new Failure_recTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
		}catch(Exception e){
			e.printStackTrace();
		}


		// Should call server task here......
		try{
			ServerSocket server_socket = new ServerSocket(10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, server_socket);
		}catch(SocketException se){
			Log.v(TAG, "Caught server exception");
			se.printStackTrace();
		}catch(IOException ie){
			Log.v(TAG, "Caught IOException in oncreate");
			ie.printStackTrace();
		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		String dbTable = "msgTable";
		String query_key = selection;
		String query_key_hashval = null;
		try{
			query_key_hashval = genHash(query_key);
		}catch(NoSuchAlgorithmException e){
			e.printStackTrace();
		}
		Cursor result_cursor = null;
		Log.v(TAG, "Querying for : "+query_key);
		if(selection.equals("@")){
			// query at any node with @, return local db cursor.
			Cursor cursor = msgHelperDb.getReadableDatabase().rawQuery("Select * from " + dbTable, null);
			result_cursor = cursor;
		}else if(selection.equals("*")){
			Request query_req = new Request();
			query_req.setAction("QUERY");
			query_req.setPresent_port(present_port);
			query_req.setNeighbors("*");
			try{
				String query_contents = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, query_req, successor_port * 2).get();
				if(("FAILED").equals(query_contents)){
					int i = chord_ring.indexOf(successor_hashval);
					if(i==4)
						i=0;
					else
						i= i+1;
					int new_suc = 5554 + 2*(node_hashes.indexOf(chord_ring.get(i)));
					query_contents = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, query_req, new_suc * 2).get();
				}
				MatrixCursor all_cursors = new MatrixCursor(new String[]{"key", "value"});
				all_cursors = get_all_contents(query_contents, all_cursors);
				Cursor my_cursor = msgHelperDb.getReadableDatabase().rawQuery("Select * from " + dbTable, null);
				my_cursor.moveToFirst();
				while (!my_cursor.isAfterLast()) {
					Object[] values = {my_cursor.getString(0), my_cursor.getString(1)};
					my_cursor.moveToNext();
					all_cursors.addRow(values);
				}
				result_cursor = all_cursors;
			}catch(Exception e){
				e.printStackTrace();
			}

		}else{
			int i=0;
			String query_coordinator_hashval = "";
			while(i<=4){
				if(i==4){
					query_coordinator_hashval = chord_ring.get(2);
					break;
				}
				if(query_key_hashval.compareTo(chord_ring.get(i)) > 0 && query_key_hashval.compareTo(chord_ring.get(i+1)) <= 0){
					if(i == 0 || i == 1){
						query_coordinator_hashval = chord_ring.get(i+3);
					}
					else if(i == 2 || i==3){
						query_coordinator_hashval = chord_ring.get(i-2);
					}
					break;
				}
				i++;
			}
			int query_coordinator_port = 5554 + 2*(node_hashes.indexOf(query_coordinator_hashval));
			Log.v(TAG, "Query coordinator port is : "+Integer.toString(query_coordinator_port));
			Request query_key_req = new Request();
			query_key_req.setAction("QUERY");
			query_key_req.setPresent_port(present_port);
			query_key_req.setNeighbors(selection);
			try{
				String query_contents = "";
				if(query_coordinator_port == Integer.parseInt(present_port)) {
					Cursor entry = msgHelperDb.getReadableDatabase().query("msgTable", null,
							"key=?", new String[]{selection}, null, null, null);
					Log.v("QUERY", "at coord only for key:" + query_key_req.getNeighbors() + " found at port: " + present_port);
					String query_key_response = "KEY NOT FOUND";
					if (entry != null) {
						entry.moveToFirst();
						if (entry.getCount() > 0)
							query_key_response = entry.getString(entry.getColumnIndex("value"));
					} else if (entry == null || entry.getCount() <= 0) {
						Log.v(TAG, "Hit Query at coord only failed");
						Request q_req = new Request();
						q_req.setAction("QUERY");
						q_req.setPresent_port(present_port);
						q_req.setNeighbors(selection);
						int ind = chord_ring.indexOf(query_coordinator_hashval);
						if(ind == 0)
							ind = 4;
						else
							ind = ind-1;
						int before_coord = 5554 + 2*(node_hashes.indexOf(chord_ring.get(ind)));
						query_key_response = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, query_key_req, before_coord * 2).get();
						if (("FAILED").equals(query_key_response)) {
							query_key_response = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, query_key_req, query_coordinator_port * 2).get();
						}
					}
					query_contents = query_key_response;
				}else{
					query_contents = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, query_key_req, query_coordinator_port * 2).get();
					if (("FAILED").equals(query_contents)) {
						int ind = chord_ring.indexOf(query_coordinator_hashval);
						if (ind == 0)
							ind = 4;
						else
							ind = ind - 1;
						int before_coord = 5554 + 2 * (node_hashes.indexOf(chord_ring.get(ind)));
						query_contents = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, query_key_req, before_coord * 2).get();
						if (("FAILED").equals(query_contents)) {
							query_contents = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, query_key_req, query_coordinator_port * 2).get();
						}
					}
					Log.v(TAG, "Fetched query contents: " + query_contents);

				}
				MatrixCursor all_cursors = new MatrixCursor(new String[]{"key", "value"});
				Object[] values = {selection, query_contents};
				all_cursors.addRow(values);
				result_cursor = all_cursors;
			}catch(Exception e){
				e.printStackTrace();
			}

		}
		return result_cursor;
	}

	public synchronized MatrixCursor get_all_contents(String data, MatrixCursor m_cursor){
		try {
			Log.v(TAG, "data sent to get_all_contents is: "+ data);
			JSONObject jsonObject = new JSONObject(data);
			JSONArray all_keys = jsonObject.getJSONArray("keys");
			JSONArray all_values = jsonObject.getJSONArray("values");

			int i=0;
			while(i < all_keys.length()){
				Object[] content_pairs = {all_keys.getString(i), all_values.getString(i)};
				m_cursor.addRow(content_pairs);
				i++;
			}
		}catch(JSONException e){
			e.printStackTrace();
		}
		return m_cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private synchronized String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private class ClientTask extends AsyncTask<Object, Void, String> {
		@Override
		protected String doInBackground(Object... msg) {
			Request req = (Request) msg[0];
			int myPort = (Integer) msg[1];
			Log.v("client task for: ",Integer.toString(myPort));
			Log.v("Req to Clienttask ",req.toString());
			String message = req.toString();
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						myPort);
				socket.setSoTimeout(500);

				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				out.println(message);

				DataInputStream in = new DataInputStream(socket.getInputStream());
				String data = "";
				data = in.readLine();
				Log.v(TAG,"received for "+msg[0]+"returned data at client task READDATA:"+ data);
				if(data == null){
					return "FAILED";
				}
				//socket.close();
				return data;

			}catch (SocketTimeoutException e) {

				Log.v(TAG, "Socket Timeout exception: " + Integer.toString(myPort));
				return "FAILED";

			} catch (UnknownHostException e) {
				Log.v(TAG, "Unknown host exception");
				e.printStackTrace();
				return "FAILED";

			} catch (IOException e) {
				Log.v(TAG, "Caught IOException: " + Integer.toString(myPort));
				e.printStackTrace();
				return "FAILED";

			}catch(Exception e){
				e.printStackTrace();
				return "FAILED";
			}
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, String> {
		@Override
		protected String doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			//Socket s;
			try {
				while (true) {
					Log.v(TAG, "just inside while true");
					Socket s = serverSocket.accept();
					String request_string = null;
					Log.v(TAG, "Inside server task");
					DataInputStream in = new DataInputStream(s.getInputStream());
					request_string  = in.readLine();
					if(request_string!=null) {
						Request req = new Request();
						Log.v(TAG, "Listening on server task");
						Log.v(TAG, "server Task| my request: " + request_string);
						String[] req_params = request_string.split(":");
						for (int k = 0; k < req_params.length; k++) {
							if (req_params[k].equals("action")) {
								k = k + 1;
								req.action = req_params[k];
								continue;
							} else if (req_params[k].equals("port")) {
								k++;
								req.present_port = req_params[k];
								continue;
							} else if (req_params[k].equals("neighbors")) {
								k++;
								req.neighbors = req_params[k];
								continue;
							}
						}
						if(req.getAction().equals("FAILURE_RECOVERY")){

							Log.v(TAG, "In recovery mode of server task from ports: "+ req.getNeighbors() + present_port);
							Log.v(TAG,"Querying @ for neighbour port");
							Cursor cursor = msgHelperDb.getReadableDatabase().rawQuery("Select * from " + "msgTable", null);
							String contents = getFinalContents(cursor,null);
							PrintWriter output = new PrintWriter(s.getOutputStream(), true);
							output.println(contents);

						}
						else if(req.getAction().equals("INSERT") || req.getAction().equals("FIRST_REPLICA") || req.getAction().equals("SECOND_REPLICA")){
							rec_lock.acquire();
							String[] key_value = req.getNeighbors().split("#");
							ContentValues content_values = new ContentValues();
							content_values.put("key", key_value[1]);
							content_values.put("value", key_value[3]);
							msgHelperDb.getWritableDatabase().insertWithOnConflict("msgTable", null, content_values, SQLiteDatabase.CONFLICT_REPLACE);
							String status = "SUCCESS";

							if (!req.getAction().equals("SECOND_REPLICA") && req.getAction().equals("INSERT")) {
								req.setAction("FIRST_REPLICA");
								req.setPresent_port(present_port);
								Log.v(TAG, "Sending msg to replica1 at port "+ successor_port);
								status = forward_message(req, 2 * successor_port,5000);
								Log.v(TAG,"Replica1 status: "+status);
								if(("FAILED").equals(status)){
									Log.v(TAG,"Writing to repli 1 failed at port: "+present_port);
									req.setAction("SECOND_REPLICA");
									int ind = chord_ring.indexOf(successor_hashval);
									if(ind ==4)
										ind = 0;
									else
										ind = ind+1;
									int suc_next = 5554 + 2*(node_hashes.indexOf(chord_ring.get(ind)));
									String status2 = forward_message(req,suc_next * 2,5000);
									if(("FAILED").equals(status2)){
										Log.v(TAG,"Failed wen coord is writing to repli2.");
										Log.v(TAG,"CoORD: "+req.present_port+" repli2 port is: "+suc_next);
										req.setAction("FIRST_REPLICA");
										status2 = forward_message(req,successor_port * 2,5000);
									}
									status = status2;
								}
							}else if(!req.getAction().equals("SECOND_REPLICA") && req.getAction().equals("FIRST_REPLICA")){
								req.setAction("SECOND_REPLICA");
								Log.v(TAG, "Sending msg to replica2 at port "+ successor_port);
								status = forward_message(req, 2 * successor_port,5000);
								Log.v(TAG,"Replica2 status: "+status);
								if(("FAILED").equals(status)){
									Log.v(TAG,"replica 2 is failed: "+successor_port);
									status = "SUCCESS";
								}
							}

							Log.v(TAG, "Sent message: "+ req.getNeighbors() +" receives response status: "+ status);
							rec_lock.release();
							PrintWriter output = new PrintWriter(s.getOutputStream(), true);
							output.println(status);

						}else if(req.getAction().equals("QUERY")){
							rec_lock.acquire();
							String data = "";
							if(req.getNeighbors().equals("*")){
								String query_response = null;
								if(Integer.parseInt(req.getPresent_port())!=successor_port){

									Request content_req = new Request();
									content_req.setAction("QUERY");
									content_req.setPresent_port(req.getPresent_port());
									content_req.setNeighbors("*");
									query_response = forward_message(content_req,successor_port * 2,5000);
									if(("FAILED").equals(query_response)){
										int i = chord_ring.indexOf(successor_hashval);
										if(i==4)
											i=0;
										else
											i= i+1;
										int new_suc = 5554 + 2*(node_hashes.indexOf(chord_ring.get(i)));
										if(Integer.parseInt(req.present_port)!=new_suc){
											query_response = forward_message(req,new_suc*2,5000);
										}
									}

								}
								Cursor cursor = msgHelperDb.getReadableDatabase().rawQuery("Select * from " + "msgTable", null);
								data = getFinalContents(cursor,query_response);
							}else{

								int p = 5554 + node_hashes.indexOf(my_hashval)*2;
								Log.v(TAG, "Querying at : "+ Integer.toString(p));
								Cursor entry = msgHelperDb.getReadableDatabase().query("msgTable", null,
										"key=?", new String[]{req.getNeighbors()}, null, null, null);
								Log.v("QUERY"," for key:"+req.getNeighbors()+" found at port: "+Integer.toString(p));
								String query_key_response = "KEY NOT FOUND";
								if(entry!=null) {
									entry.moveToFirst();
									if (entry.getCount() > 0)
										query_key_response = entry.getString(entry.getColumnIndex("value"));
								}else if(entry==null || entry.getCount() <=0){
									Request q_req = new Request();
									q_req.setAction("QUERY");
									q_req.setPresent_port(present_port);
									q_req.setNeighbors(req.getNeighbors());
									query_key_response = forward_message(q_req,predecessor_port*2,500);
									if(("FAILED").equals(query_key_response)){
										int ind = chord_ring.indexOf(predecessor_hashval);
										if(ind == 0)
											ind = 4;
										else
											ind = ind-1;
										int before_pred = 5554 + 2*(node_hashes.indexOf(chord_ring.get(ind)));
										query_key_response = forward_message(q_req,before_pred*2,500);
									}
									data = query_key_response;
								}
								data = query_key_response;
								Log.v(TAG, "Server wrting query data: "+ data);

							}
							rec_lock.release();
							PrintWriter output = new PrintWriter(s.getOutputStream(), true);
							output.println(data);

						}else if(req.getAction().equals("DELETE") || req.getAction().equals("DELETE_FIRST_REPLICA") || req.getAction().equals("DELETE_SECOND_REPLICA")){
							String response = "";
							rec_lock.acquire();
							if(req.getNeighbors().equals("*")){
								String delete_resp = "";
								if(Integer.parseInt(req.getPresent_port())!=successor_port){
									delete_resp = forward_message(req, successor_port*2,5000);
									if(("FAILED").equals(delete_resp)){
										int i = chord_ring.indexOf(successor_hashval);
										if(i==4)
											i=0;
										else
											i= i+1;
										int new_suc = 5554 + 2*(node_hashes.indexOf(chord_ring.get(i)));
										if(Integer.parseInt(req.present_port)!=new_suc){
											delete_resp = forward_message(req,new_suc*2,5000);
										}
									}

								}
								msgHelperDb.getWritableDatabase().delete("msgTable",null, null);
								response = delete_resp;
							}else{
								int p = 5554 + node_hashes.indexOf(my_hashval)*2;
								Log.v(TAG, "DEleting at : "+ Integer.toString(p));
								String delete_key_resp = "";
								int rows = msgHelperDb.getWritableDatabase().delete("msgTable","key =?",new String[]{req.getNeighbors()});


								if (!req.getAction().equals("DELETE_SECOND_REPLICA") && req.getAction().equals("DELETE")) {

									req.setAction("DELETE_FIRST_REPLICA");
									req.setPresent_port(present_port);
									Log.v(TAG, "deleteing msg at replica1 at port "+ successor_port);
									delete_key_resp = forward_message(req, 2 * successor_port,5000);
									if(("FAILED").equals(delete_key_resp)){
										req.setAction("DELETE_SECOND_REPLICA");
										int ind = chord_ring.indexOf(successor_hashval);
										if(ind ==4)
											ind = 0;
										else
											ind = ind+1;
										int suc_next = 5554 + 2*(node_hashes.indexOf(chord_ring.get(ind)));
										String status2 = forward_message(req,suc_next * 2,5000);
										delete_key_resp = status2;
									}

								}else if(!req.getAction().equals("DELETE_SECOND_REPLICA") && req.getAction().equals("DELETE_FIRST_REPLICA")){
									req.setAction("DELETE_SECOND_REPLICA");
									Log.v(TAG, "deleting msg at replica2 at port "+ successor_port);
									delete_key_resp = forward_message(req, 2 * successor_port,5000);
									delete_key_resp = "SUCCESS";
								}
								response = delete_key_resp;
							}
							rec_lock.release();
							PrintWriter output = new PrintWriter(s.getOutputStream(), true);
							output.println(response);
						}

					}else{
						Log.v(TAG, "Req String is null");
						continue;
					}
				}
			}catch(Exception e){
				Log.e(TAG, "ServerTask Exception");
				e.printStackTrace();
			}
			return null;
		}
	}


	public synchronized String getFinalContents(Cursor c, String query_resp){
		JSONObject data_json = new JSONObject();
		try {
			int length = 0;
			JSONArray all_keys = new JSONArray();
			JSONArray all_values = new JSONArray();
			if(query_resp!=null){
				try {
					JSONObject jsonObject = new JSONObject(query_resp);
					all_keys = jsonObject.getJSONArray("keys");
					all_values = jsonObject.getJSONArray("values");
					length = all_keys.length();
				} catch (JSONException e) {
					e.printStackTrace();

				}
			}
			if(c!=null) {
				c.moveToFirst();
				while (!c.isAfterLast()) {
					all_keys.put(length, c.getString(c.getColumnIndex("key")));
					all_values.put(length, c.getString(c.getColumnIndex("value")));
					length++;
					c.moveToNext();
				}
				data_json.put("keys", all_keys);
				data_json.put("values", all_values);
			}
		}catch(JSONException e){
			e.printStackTrace();
		}catch (Exception e1){
			e1.printStackTrace();
		}
		return data_json.toString();
	}

	private synchronized String forward_message(Request msg_req, int port, int timeout) {
		try {
			Log.v(TAG, "In the forward message call for msg_req: " + msg_req.toString() + " port: " + Integer.toString(port));
			Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					port);
			socket1.setSoTimeout(500);
			PrintWriter out = new PrintWriter(socket1.getOutputStream(), true);
			out.println(msg_req.toString());
			DataInputStream in = new DataInputStream(socket1.getInputStream());
			String data = "";
			data = in.readLine();
			Log.v(TAG,"Received data in forward message is: "+data);
			Log.v(TAG,"received for "+msg_req+"returned data at forward_message READDATA:"+ data);
			if(data == null)
				data= "FAILED";
			return data;

		} catch (SocketTimeoutException e) {
			Log.v(TAG,"Socket time out in forwarding message");
			e.printStackTrace();
			return "FAILED";
		} catch (IOException e) {
			Log.v(TAG,"IO exception in forwarding message");
			e.printStackTrace();
			return "FAILED";
		} catch(Exception e){
			e.printStackTrace();
			return "FAILED";
		}
	}

	private class Failure_recTask extends AsyncTask<Void, Void, Void> {

		//References: https://developer.android.com/training/data-storage/shared-preferences#java

		@Override
		protected Void doInBackground(Void... voids) {
			SharedPreferences dynamo_pref = getContext().getSharedPreferences("DynamoPref", Context.MODE_PRIVATE);
			if (!dynamo_pref.contains("OnStart")) {
				Log.v(TAG,"First start to configure preferences for port: "+present_port);
				String res = configure_preferences(dynamo_pref);
				if(res==null){
					rec_lock.release();
					return null;
				}
			}

			String predecessor_status = "";
			String successor_status = "";
			Request recovery_req = configure_request();
			Log.v(TAG, "Send msg to predecessor in recovery task");
			predecessor_status = forward_message(recovery_req,predecessor_port * 2,5000);
			if(predecessor_status.equals("FAILED")){
				Log.v(TAG, "predecessor failed");
				predecessor_status = forward_message(recovery_req,predecessor_port * 2,5000);
			}else{
				Log.v(TAG,"pred is alive and starting to recover");
				MatrixCursor local_cursor = new MatrixCursor(new String[]{"key", "value"});
				local_cursor = get_all_contents(predecessor_status, local_cursor);
				String one_before_pred = get_before_pred_node();
				String two_before_pred = getPreviousnode(one_before_pred);
				ArrayList<String> predecessors = new ArrayList<String>();
				predecessors.add(present_port);
				predecessors.add(Integer.toString(predecessor_port));
				predecessors.add(one_before_pred);
				if(local_cursor!=null) {
					local_cursor.moveToFirst();
					while (!local_cursor.isAfterLast()) {
						try{
							String key = local_cursor.getString(local_cursor.getColumnIndex("key"));
							String value = local_cursor.getString(local_cursor.getColumnIndex("value"));
							String key_hashval = genHash(key);

							int i = 0;
							String coordinator_hashval = "";
							while(i<=4){
								if(i==4){
									coordinator_hashval = chord_ring.get(0);
									break;
								}
								if(key_hashval.compareTo(chord_ring.get(i)) > 0 && key_hashval.compareTo(chord_ring.get(i+1)) <= 0){
									coordinator_hashval = chord_ring.get(i+1);
									break;
								}
								i++;
							}
							Log.v("RECOVERY | nodes","all node list for recovery for key : "+key+" "+one_before_pred+" "+two_before_pred);

							int coordinator_port = 5554 + 2*(node_hashes.indexOf(coordinator_hashval));
							if(predecessors.contains(Integer.toString(coordinator_port))){
								Log.v(TAG,"Found key space for pred for key: "+key);
								ContentValues cv = new ContentValues();
								cv.put("key", key);
								cv.put("value", value);
								msgHelperDb.getWritableDatabase().insertWithOnConflict("msgTable", null, cv, SQLiteDatabase.CONFLICT_REPLACE);
							}
							local_cursor.moveToNext();
						}catch(NoSuchAlgorithmException e){
							e.printStackTrace();
						}
					}
				}
			}

			Log.v(TAG, "Send msg to Successor in recovery task");
			successor_status = forward_message(recovery_req, successor_port* 2,5000);

			if(successor_status.equals("FAILED")){
				Log.v(TAG, "successor failed");
				successor_status = forward_message(recovery_req,successor_port * 2,5000);
			}
			else{
				Log.v(TAG,"successor is alive and starting to recover");
				MatrixCursor suc_local_cursor = new MatrixCursor(new String[]{"key", "value"});
				suc_local_cursor = get_all_contents(successor_status, suc_local_cursor);
				if(suc_local_cursor!=null) {
					suc_local_cursor.moveToFirst();
					while (!suc_local_cursor.isAfterLast()) {
						String key = suc_local_cursor.getString(suc_local_cursor.getColumnIndex("key"));
						String value = suc_local_cursor.getString(suc_local_cursor.getColumnIndex("value"));

						try {
							String key_hashval = genHash(key);

							Log.v("RECOVERY | nodes","all node list for recovery for key: "+key+" "+present_port+" "+predecessor_port);
							int i = 0;
							String coordinator_hashval = "";
							while(i<=4){
								if(i==4){
									coordinator_hashval = chord_ring.get(0);
									break;
								}
								if(key_hashval.compareTo(chord_ring.get(i)) > 0 && key_hashval.compareTo(chord_ring.get(i+1)) <= 0){
									coordinator_hashval = chord_ring.get(i+1);
									break;
								}
								i++;
							}

							int coordinator_port = 5554 + 2*(node_hashes.indexOf(coordinator_hashval));
							if(present_port.equals(Integer.toString(coordinator_port))){
								Log.v(TAG,"Found key space for successor for key: "+key);
								ContentValues cv = new ContentValues();
								cv.put("key", key);
								cv.put("value", value);
								msgHelperDb.getWritableDatabase().insertWithOnConflict("msgTable", null, cv, SQLiteDatabase.CONFLICT_REPLACE);
							}

							suc_local_cursor.moveToNext();
						}catch(NoSuchAlgorithmException e){
							e.printStackTrace();
						}
					}
				}
			}

			rec_lock.release();
			return null;
		}
	}

	private synchronized String configure_preferences(SharedPreferences d){
		Log.v(TAG, "Configuring prefs");
		SharedPreferences.Editor e = d.edit();
		e.putInt("OnStart", 0);
		e.commit();
		rec_lock.release();
		return null;
	}

	private synchronized Request configure_request(){
		Log.v(TAG,"Configuring req");
		Request rec_request = new Request();
		rec_request.setAction("FAILURE_RECOVERY");
		rec_request.setPresent_port(present_port);
		rec_request.setNeighbors(present_port);
		return rec_request;
	}


	private String getPreviousnode(String port){
		String port_hashval = "";
		try{
			port_hashval = genHash(port);
		}catch(NoSuchAlgorithmException e){
			e.printStackTrace();
		}
		int ind = chord_ring.indexOf(port_hashval);
		if(ind == 0)
			ind = 4;
		else
			ind = ind-1;

		String prev_node = Integer.toString(5554 + 2 * node_hashes.indexOf(chord_ring.get(ind)));
		return prev_node;
	}

	private  String get_before_pred_node(){

		int ind = chord_ring.indexOf(predecessor_hashval);
		if(ind == 0)
			ind = 4;
		else
			ind = ind-1;

		String before_pred = Integer.toString(5554 + 2 * node_hashes.indexOf(chord_ring.get(ind)));
		return before_pred;
	}

	private String getNextNode(){

		int ind = chord_ring.indexOf(my_hashval);
		if(ind == 4)
			ind = 0;
		else
			ind = ind+1;

		String next_node = Integer.toString(5554 + 2 * node_hashes.indexOf(chord_ring.get(ind)));
		return next_node;
	}
}