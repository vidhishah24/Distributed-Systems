package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;

public class SimpleDynamoProvider extends ContentProvider {

    static final int SERVER_PORT = 10000;
    String myPort;
    ArrayList<String> nodesList = new ArrayList(Arrays.asList("5562", "5556", "5554", "5558", "5560"));
    SQLiteDatabase sqLiteDatabase;
    String portStr;
    String[] columns = {"KEY", "VALUE"};
    private SimpleDynamoOpenHelper db;
    boolean rejoin = false;
    boolean flag;

    public SimpleDynamoProvider() throws NoSuchAlgorithmException {
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        ArrayList<String> msgArray = new ArrayList<String>();
        if (getContext().getDatabasePath("SimpleDynamo").exists()) {
            rejoin = true;
        }
        db = new SimpleDynamoOpenHelper(getContext(), null, null, 1);
        sqLiteDatabase = db.getWritableDatabase();
        System.out.println("rejoin= " + rejoin);
        try {
            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            if (getContext().getDatabasePath("SimpleDynamo").exists()) {
                sqLiteDatabase.execSQL("delete from SimpleDynamo");
                msgArray.add(0, portStr);
                msgArray.add(1, "New Join");
                System.out.println("Recover at: " + portStr);
                new NewJoinTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgArray);
            }
        } catch (IOException e) {
            System.out.println("Exception at onCreate");
            e.printStackTrace();
        }
        return false;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Context context = getContext();
        Socket socket;

        try {
            if (selection.equals("*") || selection.equals("\"*\"")) {
                String[] msgArray = new String[5];
                for (int i = 0; i < nodesList.size(); i++) {
                    msgArray[1] = "Delete All";
                    msgArray[0] = nodesList.get(i);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(nodesList.get(i)) * 2);
                    OutputStream os = socket.getOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(os);
                    oos.writeObject(msgArray);
                }
            } else if (selection.equals("@") || selection.equals("\"@\"")) {
                sqLiteDatabase.delete("SimpelDynamo", null, null);
            } else {
                System.out.println("DELETE");
                for (int i = 0; i < nodesList.size() - 1; i++) {
                    if (genHash(nodesList.get(i)).compareTo(genHash(selection)) < 0 && genHash(nodesList.get(i + 1)).compareTo(genHash(selection)) >= 0) {
                        for (int j = i + 1; j < i + 4; j++) {
                            ArrayList<String> msgArray = new ArrayList<String>();
                            if (j < nodesList.size()) {

                                msgArray.add(0, nodesList.get(j));
                                msgArray.add(1, "Delete");
                                msgArray.add(2, selection);
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(msgArray.get(0)) * 2);
                                OutputStream os = socket.getOutputStream();
                                ObjectOutputStream oos = new ObjectOutputStream(os);
                                oos.writeObject(msgArray);
                            } else {
                                msgArray.add(0, nodesList.get(j - nodesList.size()));
                                msgArray.add(1, "Delete");
                                msgArray.add(2, selection);
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(msgArray.get(0)) * 2);
                                OutputStream os = socket.getOutputStream();
                                ObjectOutputStream oos = new ObjectOutputStream(os);
                                oos.writeObject(msgArray);
                            }
                        }
//                        break;
                    } else if (genHash(nodesList.get(nodesList.size() - 1)).compareTo(genHash(selection)) < 0 || genHash(nodesList.get(0)).compareTo(genHash(selection)) >= 0) {
                        for (int j = 0; j < 3; j++) {
                            ArrayList<String> msgArray = new ArrayList<String>();
                            msgArray.add(0, nodesList.get(j));
                            msgArray.add(1, "Delete");
                            msgArray.add(2, selection);
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(nodesList.get(j)) * 2);
                            OutputStream os = socket.getOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(os);
                            oos.writeObject(msgArray);
                        }

                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Exception at Delete");
            e.printStackTrace();
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
            boolean flag = true;
            System.out.println("At Insert");
            String key = (String) values.get("key");
            String value = (String) values.get("value");
            Thread.sleep(200);
            for (int i = 0; i < nodesList.size() - 1; i++) {
                if (genHash(nodesList.get(i)).compareTo(genHash(key)) < 0 && genHash(nodesList.get(i + 1)).compareTo(genHash(key)) >= 0) {
                    for (int j = i + 1; j < i + 4; j++) {
                        ArrayList<String> msgArray = new ArrayList<String>();
                        if (j < nodesList.size()) {
                            msgArray.add(0, nodesList.get(j));
                            msgArray.add(1, "INSERT");
                            msgArray.add(2, key);
                            msgArray.add(3, value);
                            msgArray.add(4, Integer.toString(i + 1));
                            System.out.println("At Insert: " + msgArray.get(0) + " " + msgArray.get(1) + " " + msgArray.get(2) + " " + msgArray.get(3) + " " + msgArray.get(4));
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgArray);
                        } else {
                            msgArray.add(0, nodesList.get(j - nodesList.size()));
                            msgArray.add(1, "INSERT");
                            msgArray.add(2, key);
                            msgArray.add(3, value);
                            msgArray.add(4, Integer.toString(i + 1));
                            System.out.println("At Insert: " + msgArray.get(0) + " " + msgArray.get(1) + " " + msgArray.get(2) + " " + msgArray.get(3) + " " + msgArray.get(4));
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgArray);
                        }
                    }
                    break;
                } else if (genHash(nodesList.get(nodesList.size() - 1)).compareTo(genHash(key)) < 0 || genHash(nodesList.get(0)).compareTo(genHash(key)) >= 0) {
                    for (int j = 0; j < 3; j++) {
                        ArrayList<String> msgArray = new ArrayList<String>();
                        msgArray.add(0, nodesList.get(j));
                        msgArray.add(1, "INSERT");
                        msgArray.add(2, key);
                        msgArray.add(3, value);
                        msgArray.add(4, Integer.toString(0));
                        System.out.println("At Insert: " + msgArray.get(0) + " " + msgArray.get(1) + " " + msgArray.get(2) + " " + msgArray.get(3) + " " + msgArray.get(4));
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgArray);
                    }
                    break;
                }

            }
            flag = false;
        } catch (Exception e) {
            System.out.println("Exception at insert");
            e.printStackTrace();
        }

        System.out.println("Exit Insert");
        return null;

    }


    @Override
    public synchronized Cursor query(Uri uri, String[] projection, String selection,
                                     String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
//        String[] msgArray = new String[5];
        while (flag) {
        }
        ArrayList<String> msgArray = new ArrayList<String>();
        MatrixCursor cursor = new MatrixCursor(columns);
        SQLiteDatabase sqLiteDatabase = db.getReadableDatabase();
        Socket socket;
        try {
            if (selection.equals("*") || selection.equals("\"*\"")) {
                for (int i = 0; i < nodesList.size(); i++) {
                    try {
                        msgArray.add(0, nodesList.get(i));
                        msgArray.add(1, "Query All");
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(nodesList.get(i)) * 2);
                        OutputStream os = socket.getOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(os);
                        oos.writeObject(msgArray);
                        InputStream is = socket.getInputStream();
                        ObjectInputStream ois = new ObjectInputStream(is);
                        ArrayList<String[]> cursorObj = (ArrayList<String[]>) ois.readObject();
                        for (int j = 0; j < cursorObj.size(); j++) {
                            String[] cursorArray = {cursorObj.get(j)[0], cursorObj.get(j)[1]};
                            cursor.addRow(cursorArray);
                        }
                    } catch (Exception e) {
                        System.out.println("At * query Exception");
                    }
                }
                return cursor;

            } else if (selection.equals("@") || selection.equals("\"@\"")) {
                Thread.sleep(2500);
                System.out.println("Started querying");
                Cursor cursor1 = sqLiteDatabase.rawQuery("SELECT key,value from SimpleDynamo", null);
                return cursor1;

            } else {
                System.out.println("At Individual query" + " " + selection + " " + genHash(selection));
                for (int i = 0; i < nodesList.size() - 1; i++) {
                    if (genHash(nodesList.get(i)).compareTo(genHash(selection)) < 0 && genHash(nodesList.get(i + 1)).compareTo(genHash(selection)) >= 0) {
                        msgArray.add(0, nodesList.get(i + 1));
                        msgArray.add(1, "Query");
                        msgArray.add(2, selection);
                        ArrayList<String[]> cursorObj = new ArrayList<String[]>();
                        try {
                            cursorObj = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgArray).get();
                            int k = i + 1;
                            while (cursorObj == null || cursorObj.size() == 0) {
                                System.out.println("Inside while");
                                k = k + 1;
                                if (k >= nodesList.size()) {
                                    msgArray.add(0, nodesList.get((k) - nodesList.size()));
                                    msgArray.add(1, "Query");
                                    msgArray.add(2, selection);
                                } else {
                                    msgArray.set(0, nodesList.get(i + 2));
                                    msgArray.set(1, "Query");
                                    msgArray.set(2, selection);
                                }
                                System.out.println("After Updating: " + msgArray.get(0) + " " + msgArray.get(1) + " " + msgArray.get(2));
                                cursorObj = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgArray).get();
                            }
                        } catch (Exception e) {
                            System.out.println("Exception at query");
                            e.printStackTrace();
                            if ((i + 2) >= nodesList.size()) {
                                msgArray.add(0, nodesList.get((i + 2) - nodesList.size()));
                                msgArray.add(1, "Query");
                                msgArray.add(2, selection);
                            } else {
                                msgArray.set(0, nodesList.get(i + 2));
                                msgArray.set(1, "Query");
                                msgArray.set(2, selection);
                            }
                            System.out.println("After Updating at Exception: " + msgArray.get(0) + " " + msgArray.get(1) + " " + msgArray.get(2));
                            cursorObj = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgArray).get();
                        }
                        String[] cursorArray = {cursorObj.get(0)[0], cursorObj.get(0)[1]};
                        cursor.addRow(cursorArray);
                        return cursor;
                    } else if (genHash(nodesList.get(nodesList.size() - 1)).compareTo(genHash(selection)) < 0 || genHash(nodesList.get(0)).compareTo(genHash(selection)) >= 0) {
                        msgArray.add(0, nodesList.get(0));
                        msgArray.add(1, "Query");
                        msgArray.add(2, selection);
                        ArrayList<String[]> cursorObj;
                        try {
                            cursorObj = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgArray).get();

                            int k = 0;
                            while (cursorObj == null || cursorObj.size() == 0) {
                                k = k + 1;
                                System.out.println("Inside while");
                                msgArray.set(0, nodesList.get(k));
                                msgArray.set(1, "Query");
                                msgArray.set(2, selection);
                                System.out.println("After Updating: " + msgArray.get(0) + " " + msgArray.get(1) + " " + msgArray.get(2));
                                cursorObj = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgArray).get();
                            }
                        } catch (Exception e) {
                            System.out.println("Exception at query");
                            e.printStackTrace();

                            msgArray.set(0, nodesList.get(1));
                            msgArray.set(1, "Query");
                            msgArray.set(2, selection);
                            System.out.println("After Updating at Exception: " + msgArray.get(0) + " " + msgArray.get(1) + " " + msgArray.get(2));
                            cursorObj = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgArray).get();
                        }
                        String[] cursorArray = {cursorObj.get(0)[0], cursorObj.get(0)[1]};
                        cursor.addRow(cursorArray);
                        System.out.println("cursorcount: " + cursor.getCount() + " " + selection);
                        return cursor;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Exception at query in last");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket soc = serverSocket.accept();
                    InputStream is = soc.getInputStream();
                    ObjectInputStream ois = new ObjectInputStream(is);
                    ArrayList<String> msgArray1 = (ArrayList<String>) ois.readObject();
                    if (msgArray1.size() > 1) {
                        System.out.println("At Server: " + msgArray1.get(0) + " " + msgArray1.get(1));
                    } else
                        System.out.println("At Server: " + msgArray1.get(0));
                    ArrayList<String[]> cursor_keyvalue = new ArrayList<String[]>();
                    if (msgArray1.get(0).equals("JOINED")) {
                        System.out.println("Joined");
                        ContentValues values = new ContentValues();
                        SQLiteDatabase sqLiteDatabase = db.getWritableDatabase();
                        msgArray1.remove(0);
                        for (int i = 0; i < msgArray1.size(); i++) {
                            String keyValue[] = msgArray1.get(i).split("#");
                            values.put("key", keyValue[0]);
                            values.put("value", keyValue[1]);
                            values.put("position", (keyValue[2]));
                            System.out.println("Joined: " + keyValue[0] + " " + keyValue[1] + " " + keyValue[2]);
                            long row = sqLiteDatabase.insert("SimpleDynamo", null, values);
                            System.out.println("row at insert: " + row);
                        }
                    } else if (msgArray1.get(1).equals("INSERT")) {
                        SQLiteDatabase sqLiteDatabase = db.getWritableDatabase();
                        SQLiteDatabase sqLitereadDatabase = db.getReadableDatabase();
                        System.out.println("AT Insert in server");
                        System.out.println("keyvalueindex at server: " + msgArray1.get(2) + " " + msgArray1.get(3) + " " + msgArray1.get(4));
                        Cursor cursor = sqLitereadDatabase.rawQuery("SELECT * from SimpleDynamo WHERE key=?", new String[]{msgArray1.get(2)});
                        if (cursor.getCount() > 0) {
                            sqLiteDatabase.delete("SimpleDynamo", "KEY='" + msgArray1.get(2) + "'", null);
                        }
                        ContentValues values = new ContentValues();
                        values.put("key", msgArray1.get(2));
                        values.put("value", msgArray1.get(3));
                        values.put("position", (msgArray1.get(4)));
                        sqLiteDatabase.insert("SimpleDynamo", null, values);
                    } else if (msgArray1.get(1).equals("Query")) {
                        System.out.println("At Query in Server " + msgArray1.get(2));
                        SQLiteDatabase sqLitereadDatabase = db.getReadableDatabase();
                        Cursor cursor = sqLitereadDatabase.rawQuery("SELECT * from SimpleDynamo WHERE key=?", new String[]{msgArray1.get(2)});
                        cursor.moveToFirst();
                        String cursor_key = cursor.getString(cursor.getColumnIndex(cursor.getColumnName(0)));
                        String cursor_value = cursor.getString(cursor.getColumnIndex(cursor.getColumnName(1)));
                        String[] temp = new String[2];
                        temp[0] = cursor_key;
                        temp[1] = (cursor_value);
                        cursor_keyvalue.add(temp);
                        System.out.println("Giving Query back from Server " + msgArray1.get(2) + " " + cursor_keyvalue.size());
                        OutputStream os = soc.getOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(os);
                        oos.writeObject(cursor_keyvalue);

                    } else if (msgArray1.get(1).equals("Query All")) {
                        SQLiteDatabase sqLitereadDatabase = db.getReadableDatabase();
                        Cursor cursor = sqLitereadDatabase.rawQuery("SELECT * from SimpleDynamo", null);
                        cursor.moveToFirst();
                        do {
                            if (cursor.getCount() > 0) {
                                String cursor_key = cursor.getString(cursor.getColumnIndex("key"));
                                String cursor_value = cursor.getString(cursor.getColumnIndex("value"));
                                String[] temp = new String[2];
                                temp[0] = (cursor_key);
                                temp[1] = (cursor_value);
                                cursor_keyvalue.add(temp);
                            }
                        } while (cursor.moveToNext());
                        OutputStream os = soc.getOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(os);
                        oos.writeObject(cursor_keyvalue);
                    } else if (msgArray1.get(1).equals("Delete All")) {
                        SQLiteDatabase sqLiteDatabase = db.getReadableDatabase();
                        sqLiteDatabase.delete("SimpleDynamo", null, null);
                    } else if (msgArray1.get(1).equals("Delete")) {
                        SQLiteDatabase sqLiteDatabase = db.getReadableDatabase();
                        sqLiteDatabase.delete("SimpleDynamo", "KEY='" + msgArray1.get(2) + "'", null);
                    } else if (msgArray1.get(1).equals("New Join")) {
                        System.out.println("New Join");
                        SQLiteDatabase sqLiteDatabase = db.getReadableDatabase();

                        ArrayList<String> list = new ArrayList<String>();
                        list.add("JOINED");
                        Cursor cursor = sqLiteDatabase.rawQuery("SELECT * from SimpleDynamo", null);
                        cursor.moveToFirst();
                        do {
                            if (cursor.getCount() > 0) {
                                String key = cursor.getString(cursor.getColumnIndex("key"));
                                String value = cursor.getString(cursor.getColumnIndex("value"));
                                String index = cursor.getString(cursor.getColumnIndex("position"));
                                String temp = key + "#" + value + "#" + index;

                                int failedport = nodesList.indexOf(msgArray1.get(0));
                                int prev1 = nodesList.indexOf(msgArray1.get(0)) - 1;
                                int prev2 = nodesList.indexOf(msgArray1.get(0)) - 2;
                                int next = nodesList.indexOf(msgArray1.get(0)) + 1;
                                if (prev1 < 0) {
                                    prev1 = nodesList.size() + prev1;
                                }
                                if (prev2 < 0) {
                                    prev2 = nodesList.size() + prev2;
                                }
                                if (next >= nodesList.size()) {
                                    next = next - nodesList.size();
                                }
                                System.out.println("port itself: " + portStr + " Failed Port: " + nodesList.indexOf(msgArray1.get(0)) + " prev: " + prev1 + " next: " + next);
                                if (nodesList.indexOf(portStr) == prev1) {
                                    System.out.println("prev1");
                                    if (index.equals(Integer.toString(prev1))) {
                                        System.out.println("prev1 if");
                                        list.add(temp);
                                    }
                                } else if (nodesList.indexOf(portStr) == prev2) {
                                    System.out.println("prev2");
                                    if (index.equals(Integer.toString(prev2))) {
                                        System.out.println("prev2 if");
                                        list.add(temp);
                                    }
                                } else if (nodesList.indexOf(portStr) == next) {
                                    System.out.println("next");
                                    if (index.equals(Integer.toString(failedport))) {
                                        System.out.println("next if");
                                        list.add(temp);
                                    }
                                }
                            }
                        } while (cursor.moveToNext());
                        System.out.println("List size at " + list.size() + " " + portStr + " for " + msgArray1.get(0));
                        System.out.println("keys for: " + myPort);
                        for (int i = 0; i < list.size(); i++) {
                            System.out.println("List at server " + list.get(i));
                        }
                        OutputStream os = soc.getOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(os);
                        oos.writeObject(list);
                    }
                } catch (Exception e) {
                    System.out.println("Exception at server");
                    e.printStackTrace();
                }
            }
        }
    }

    private class NewJoinTask extends AsyncTask<ArrayList<String>, Void, Void> {

        @Override
        protected Void doInBackground(ArrayList<String>... msgs) {
            System.out.println("NewJoinTask");
            Socket socket = null;
            for (int i = 0; i < nodesList.size(); i++) {
                if (nodesList.get(i).equals(msgs[0].get(0))) {
                } else {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(nodesList.get(i)) * 2);
                        OutputStream os = socket.getOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(os);
                        oos.writeObject(msgs[0]);

                    } catch (Exception e) {
                        System.out.println("Exception at New Join CLient");
                    }
                    try {
                        System.out.println("reading list");
                        InputStream is = socket.getInputStream();
                        ObjectInputStream ois = new ObjectInputStream(is);
                        ArrayList<String> list = (ArrayList<String>) ois.readObject();
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgs[0].get(0)) * 2);
                        OutputStream os = socket.getOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(os);
                        oos.writeObject(list);
                    } catch (Exception e) {
                        System.out.println("Exception at NewJoin Client");
                        e.printStackTrace();
                    }
                }
            }
            return null;
        }
    }

    private class ClientTask extends AsyncTask<ArrayList<String>, Void, Object> {

        @Override
        protected Object doInBackground(ArrayList<String>... msgs) {
            Socket socket = null;
            try {
                if (msgs[0].get(1).equals("INSERT")) {

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[0].get(0)) * 2);
                    OutputStream os = socket.getOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(os);
                    oos.writeObject(msgs[0]);

                }
                if (msgs[0].get(1).equals("Query")) {
                    System.out.println("Query at Client " + msgs[0].get(2));
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[0].get(0)) * 2);
                    OutputStream os = socket.getOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(os);
                    oos.writeObject(msgs[0]);
                    InputStream is = socket.getInputStream();
                    ObjectInputStream ois = new ObjectInputStream(is);
                    return ois.readObject();
                }
            } catch (OptionalDataException e) {
                System.out.println("Exception 1");
                e.printStackTrace();
            } catch (UnknownHostException e) {
                System.out.println("Exception 2");
                e.printStackTrace();
            } catch (StreamCorruptedException e) {
                System.out.println("Exception 3");
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("Exception 4");
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                System.out.println("Exception 5");
                e.printStackTrace();
            }
            return null;
        }
    }

}


class SimpleDynamoOpenHelper extends SQLiteOpenHelper {

    private static final int DATABASE_VERSION = 1;
    private static final String SIMPLE_DYNAMO_TABLE_CREATE =
            "CREATE TABLE SimpleDynamo (key TEXT, value TEXT, position TEXT);";

    public SimpleDynamoOpenHelper(Context context, String name,
                                  SQLiteDatabase.CursorFactory factory, int version) {
        super(context, "SimpleDynamo", factory, DATABASE_VERSION);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        Log.v("db onUpgrade", "SimpleDynamo");
        db.execSQL("DROP TABLE IF EXISTS SimpleDynamo");
        onCreate(db);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS SimpleDynamo");
        db.execSQL(SIMPLE_DYNAMO_TABLE_CREATE);
        Log.v("db onCreate", "SimpleDynamo");
    }
}