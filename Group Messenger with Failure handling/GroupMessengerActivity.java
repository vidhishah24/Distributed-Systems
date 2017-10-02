package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerProvider.class.getSimpleName();
    static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    public static ArrayList<String> msgList = new ArrayList<String>();
    static int counter = 0;
    static double seq = 0;
    public String messageString = "";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        final EditText editText = (EditText) findViewById(R.id.editText1);
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        final Button button = (Button) findViewById(R.id.button4);
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString();
                editText.setText(""); // This is one way to reset the input box.
                TextView localTextView = (TextView) findViewById(R.id.textView1);
                localTextView.append("\t" + msg); // This is one way to display a string.
                TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                remoteTextView.append("\n");
                counter++;
                messageString = msg + ":" + counter + ":" + myPort;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageString);
                // return true;
            }
        });

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
            while (true) {
                try {
                    Socket soc = serverSocket.accept();
                    DataInputStream input = new DataInputStream(soc.getInputStream());
                    String msg = input.readUTF();
                    if (msg.substring(msg.lastIndexOf(":") + 1, msg.length()).equals("FALSE")) {
                        msg = msg.substring(0, msg.lastIndexOf(":"));
                        seq = seq + 1;
                        msg = msg + ":" + seq;
                        DataOutputStream d = new DataOutputStream(soc.getOutputStream());
                        d.writeUTF(msg);
                    } else if (msg.substring(msg.lastIndexOf(":") + 1, msg.length()).equals("TRUE")) {
                        msg = msg.substring(0, msg.lastIndexOf(":"));
                        String msgArray[] = msg.split(":");
                        ContentValues keyValue = new ContentValues();
                        keyValue.put("key", Integer.toString((int) (Double.parseDouble(msgArray[4]) - 1)));
                        keyValue.put("value", msgArray[0]);
                        getContentResolver().insert(uri, keyValue);
                        publishProgress(msgArray[0]);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            TextView localTextView = (TextView) findViewById(R.id.textView1);
            localTextView.append("\n");
            String filename = "GroupMessenger1Output";
            String string = strReceived + "\n";
            FileOutputStream outputStream;
            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            Log.v(TAG, "at client");
            for (int i = 0; i < REMOTE_PORT.length; i++) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT[i]));
                    msgs[0] = msgs[0] + ":" + REMOTE_PORT[i];
                    String msgToSend = msgs[0];
                    DataOutputStream d = new DataOutputStream(socket.getOutputStream());
                    d.writeUTF(msgToSend + ":FALSE");
                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    String msgWithSequence = input.readUTF();
                    msgList.add(msgWithSequence);
                    msgs[0] = msgs[0].substring(0, msgs[0].lastIndexOf(":"));
                } catch (Exception e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                    msgs[0] = msgs[0].substring(0, msgs[0].lastIndexOf(":"));
                }
            }

            //if (msgList.size() == REMOTE_PORT.length) {
            String maxString = sortList.sortArrayList(msgList);

            for (int i = 0; i < REMOTE_PORT.length; i++) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT[i]));
                    DataOutputStream d = new DataOutputStream(socket.getOutputStream());
                    d.writeUTF(maxString + ":TRUE");
                } catch (Exception e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                }
            }
            msgList.clear();
            //  }

            return null;
        }
    }
}


