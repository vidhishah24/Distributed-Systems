package edu.buffalo.cse.cse486586.simpledht;

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
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    String[] msgArray = new String[4];
    String myPort;
    SQLiteDatabase sqLiteDatabase;
    TreeMap<String, String[]> nodes = new TreeMap<String, String[]>();
    String[] columns = {"KEY", "VALUE"};

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Context context = getContext();
        SQLiteStorage sqLiteStorage = new SQLiteStorage(context);
        Socket socket;
        String[] nodeArray = nodes.keySet().toArray(new String[nodes.size()]);
        SQLiteDatabase sqLiteDatabase = sqLiteStorage.getWritableDatabase();
        if (nodes.size() < 2) {
            if (selection.equals("*") || selection.equals("@")) {
                sqLiteDatabase.delete(SQLiteStorage.DATABASE_NAME, null, null);
            } else {
                sqLiteDatabase.delete(SQLiteStorage.DATABASE_NAME, "KEY='" + selection + "'", null);
            }

        } else {
            try {
                if (selection.equals("*")) {
                    for (String nodeKeys : nodes.keySet()) {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(nodes.get(nodeKeys)[0]) * 2);
                        nodes.get(nodeKeys)[1] = "Delete All";
                        OutputStream os = socket.getOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(os);
                        oos.writeObject(nodes);
                    }
                } else if (selection.equals("@")) {
                    sqLiteDatabase.delete(SQLiteStorage.DATABASE_NAME, null, null);
                } else {
                    for (int i = 0; i < nodeArray.length - 1; i++) {
                        if (nodeArray[i].compareTo(genHash(selection)) < 0 && nodeArray[i + 1].compareTo(genHash(selection)) >= 0) {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(nodes.get(nodeArray[i + 1])[0]) * 2);
                            msgArray[0] = nodes.get(nodeArray[i + 1])[0];
                            msgArray[1] = "Delete";
                            msgArray[2] = selection;
                            TreeMap<String, String[]> newNode = new TreeMap<String, String[]>();
                            newNode.put(nodeArray[i + 1], msgArray);
                            OutputStream os = socket.getOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(os);
                            oos.writeObject(newNode);
                        } else if (nodeArray[nodes.size() - 1].compareTo(genHash(selection)) < 0 || nodeArray[0].compareTo(genHash(selection)) >= 0) {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(nodes.get(nodeArray[0])[0]) * 2);
                            nodes.get(nodeArray[0])[1] = "Delete";
                            nodes.get(nodeArray[0])[2] = selection;
                            OutputStream os = socket.getOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(os);
                            oos.writeObject(nodes);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        Socket socket = null;
        try {

            String key = (String) values.get("key");
            String value = (String) values.get("value");
            TreeMap<String, String[]> insertposition;
            values.put(SQLiteStorage.KEY, key);
            values.put(SQLiteStorage.VALUE, value);
            msgArray[1] = "Insert";
            String[] nodeArray = nodes.keySet().toArray(new String[nodes.size()]);
            if (nodes.size() < 2) {
                sqLiteDatabase.insert(SQLiteStorage.DATABASE_NAME, null, values);
            } else {
                for (int i = 0; i < nodeArray.length - 1; i++) {
                    try {
                        if (nodeArray[i].compareTo(genHash(key)) < 0 && nodeArray[i + 1].compareTo(genHash(key)) >= 0) {
                            insertposition = new TreeMap<String, String[]>();
                            String insertKeyValue[] = {key, "Insert", value};
                            insertposition.put(nodeArray[i + 1], insertKeyValue);
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(nodes.get(nodeArray[i + 1])[0]) * 2);

                            OutputStream os = socket.getOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(os);
                            oos.writeObject(insertposition);
                        } else if (nodeArray[nodes.size() - 1].compareTo(genHash(key)) < 0 || nodeArray[0].compareTo(genHash(key)) >= 0) {
                            insertposition = new TreeMap<String, String[]>();
                            String insertKeyValue[] = {key, "Insert", value};
                            insertposition.put(nodeArray[0], insertKeyValue);
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(nodes.get(nodeArray[0])[0]) * 2);

                            OutputStream os = socket.getOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(os);
                            oos.writeObject(insertposition);
                            ;
                        }
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
            }
            Log.v("insert", values.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Context context = getContext();
        SQLiteStorage sqLiteStorage = new SQLiteStorage(context);
        sqLiteDatabase = sqLiteStorage.getWritableDatabase();
        try {
            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            if (!portStr.equals("5554")) {
                msgArray[0] = portStr;
                msgArray[1] = "JoinRequest";
                nodes.put(genHash(portStr), msgArray);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodes);
            } else {
                msgArray[0] = portStr;
                nodes.put(genHash(portStr), msgArray);
            }

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        MatrixCursor cursor = new MatrixCursor(columns);
        Context context = getContext();
        SQLiteStorage sqLiteStorage = new SQLiteStorage(context);
        SQLiteDatabase sqLiteDatabase = sqLiteStorage.getReadableDatabase();
        Socket socket;
        String[] nodeArray = nodes.keySet().toArray(new String[nodes.size()]);
        if (nodes.size() < 2) {
            if (selection.equals("*") || selection.equals("@")) {
                return sqLiteDatabase.rawQuery("SELECT * from SimpleDht", null);
            } else
                return sqLiteDatabase.rawQuery("SELECT * from SimpleDht WHERE key=?", new String[]{selection});
        } else {
            try {
                if (selection.equals("*")) {
                    for (String nodeKeys : nodes.keySet()) {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(nodes.get(nodeKeys)[0]) * 2);
                        nodes.get(nodeKeys)[1] = "Query All";
                        OutputStream os = socket.getOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(os);
                        oos.writeObject(nodes);
                        InputStream is = socket.getInputStream();
                        ObjectInputStream ois = new ObjectInputStream(is);
                        HashMap<String, String> cursorobj = (HashMap<String, String>) ois.readObject();
                        for (String keys : cursorobj.keySet()) {
                            String[] cursorArray = {keys, cursorobj.get(keys)};
                            cursor.addRow(cursorArray);
                        }
                    }
                    return cursor;

                } else if (selection.equals("@")) {
                    return sqLiteDatabase.rawQuery("SELECT * from SimpleDht", null);

                } else {
                    for (int i = 0; i < nodeArray.length - 1; i++) {
                        if (nodeArray[i].compareTo(genHash(selection)) < 0 && nodeArray[i + 1].compareTo(genHash(selection)) >= 0) {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(nodes.get(nodeArray[i + 1])[0]) * 2);
                            msgArray[0] = nodes.get(nodeArray[i + 1])[0];
                            msgArray[1] = "Query";
                            msgArray[2] = selection;
                            TreeMap<String, String[]> newnode = new TreeMap<String, String[]>();
                            newnode.put(nodeArray[i + 1], msgArray);
                            OutputStream os = socket.getOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(os);
                            oos.writeObject(newnode);

                            InputStream is = socket.getInputStream();
                            ObjectInputStream ois = new ObjectInputStream(is);
                            HashMap<String, String> cursorObj = (HashMap<String, String>) ois.readObject();
                            Map.Entry<String, String> entry = cursorObj.entrySet().iterator().next();
                            String[] cursorArray = {entry.getKey(), entry.getValue()};
                            cursor.addRow(cursorArray);
                            return cursor;
                        } else if (nodeArray[nodes.size() - 1].compareTo(genHash(selection)) < 0 || nodeArray[0].compareTo(genHash(selection)) >= 0) {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(nodes.get(nodeArray[0])[0]) * 2);

                            nodes.get(nodeArray[0])[1] = "Query";
                            nodes.get(nodeArray[0])[2] = selection;
                            OutputStream os = socket.getOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(os);
                            oos.writeObject(nodes);
                            InputStream is = socket.getInputStream();
                            ObjectInputStream ois = new ObjectInputStream(is);
                            HashMap<String, String> cursorObj = (HashMap<String, String>) ois.readObject();
                            Map.Entry<String, String> entry = cursorObj.entrySet().iterator().next();
                            String[] cursorArray = {entry.getKey(), entry.getValue()};
                            cursor.addRow(cursorArray);
                            return cursor;
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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
                    TreeMap<String, String[]> myMessage = (TreeMap<String, String[]>) ois.readObject();
                    Map.Entry<String, String[]> entry = myMessage.entrySet().iterator().next();
                    SQLiteStorage sqLiteStorage = new SQLiteStorage(getContext());
                    SQLiteDatabase sqLitereadDatabase = sqLiteStorage.getReadableDatabase();
                    HashMap<String, String> cursor_keyvalue = new HashMap<String, String>();
                    if (entry.getValue()[1].equals("JoinRequest")) {
                        String mymessageArray[] = entry.getValue();
                        String hashPort = genHash(entry.getValue()[0]);
                        nodes.put(hashPort, mymessageArray);
                        OutputStream os = soc.getOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(os);
                        oos.writeObject(nodes);
                    } else if (entry.getValue()[1].equals("JoinReply")) {
                        nodes = (TreeMap) myMessage.clone();
                        for (String keys : nodes.keySet()) {
                            nodes.get(keys)[1] = "Joined";
                        }
                    } else if (entry.getValue()[1].equals("Query All")) {
                        Cursor cursor = sqLitereadDatabase.rawQuery("SELECT * from SimpleDht", null);
                        cursor.moveToFirst();
                        do {
                            if (cursor.getCount() > 0) {
                                String cursor_key = cursor.getString(cursor.getColumnIndex("key"));
                                String cursor_value = cursor.getString(cursor.getColumnIndex("value"));
                                cursor_keyvalue.put(cursor_key, cursor_value);
                            }
                        } while (cursor.moveToNext());
                        OutputStream os = soc.getOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(os);
                        oos.writeObject(cursor_keyvalue);

                    } else if (entry.getValue()[1].equals("Query")) {
                        Cursor cursor = sqLitereadDatabase.rawQuery("SELECT * from SimpleDht WHERE key=?", new String[]{entry.getValue()[2]});
                        cursor.moveToFirst();
                        String cursor_key = cursor.getString(cursor.getColumnIndex(cursor.getColumnName(0)));
                        String cursor_value = cursor.getString(cursor.getColumnIndex(cursor.getColumnName(1)));
                        cursor_keyvalue.put(cursor_key, cursor_value);
                        OutputStream os = soc.getOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(os);
                        oos.writeObject(cursor_keyvalue);
                    } else if (entry.getValue()[1].equals("Insert")) {
                        ContentValues values = new ContentValues();
                        values.put(SQLiteStorage.KEY, entry.getValue()[0]);
                        values.put(SQLiteStorage.VALUE, entry.getValue()[2]);
                        sqLiteDatabase.insert(SQLiteStorage.DATABASE_NAME, null, values);
                    } else if (entry.getValue()[1].equals("Delete All")) {
                        sqLiteDatabase.delete(SQLiteStorage.DATABASE_NAME, null, null);
                    } else if (entry.getValue()[1].equals("Delete")) {
                        sqLiteDatabase.delete(SQLiteStorage.DATABASE_NAME, "KEY='" + entry.getValue()[2] + "'", null);
                    }


                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private class ClientTask extends AsyncTask<TreeMap<String, String[]>, Void, Void> {

        @Override
        protected Void doInBackground(TreeMap<String, String[]>... msgs) {
            Socket socket = null;
            try {
                Map.Entry<String, String[]> entry = msgs[0].entrySet().iterator().next();
                if (entry.getValue()[1].equals("JoinRequest")) {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt("5554") * 2);
                    OutputStream os = socket.getOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(os);
                    oos.writeObject(msgs[0]);
                }
                InputStream is = socket.getInputStream();
                ObjectInputStream ois = new ObjectInputStream(is);
                TreeMap<String, String[]> ring = (TreeMap<String, String[]>) ois.readObject();
                for (String name : ring.keySet()) {
                    ring.get(name)[1] = "JoinReply";
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(ring.get(name)[0]) * 2);
                    OutputStream os = socket.getOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(os);
                    oos.writeObject(ring);
                }


            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}


class SQLiteStorage extends SQLiteOpenHelper {

    public static final String DATABASE_NAME = "SimpleDht";
    public static final String KEY = "key";
    public static final String VALUE = "value";
    private static final int DATABASE_VERSION = 1;
    private static final String DATABASE_CREATE_TABLE = "CREATE TABLE " + DATABASE_NAME + " (" + KEY + " TEXT, " + VALUE + " TEXT);";

    SQLiteStorage(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(DATABASE_CREATE_TABLE);
    }

}

