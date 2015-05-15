package Collect;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import com.ib.client.EClientSocket;
import com.ib.client.EWrapper;
import com.ib.client.Execution;
import com.ib.client.Order;
import com.ib.client.OrderState;
import com.ib.client.UnderComp;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public abstract class unicycleWrapper implements EWrapper {

    public abstract void opened();
    public abstract void closed();
    public abstract void reqComplete(int id);
//     public abstract void errorCode(int code);

    private static final int MAX_MESSAGES = 1000000;

    // main client
    private EClientSocket m_client;
    private int m_clientId = getInt("Client.Id");

    // utils
    private ResourceBundle bundle;
    private long ts;
    private PrintStream messageStream;
//     private int m_messageCounter;	
    private final SimpleDateFormat m_df = new SimpleDateFormat("HH:mm:ss"); 
    private Contract contract;
    private boolean reconnect = false;
    private DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
    private Boolean header = null;

    private String arg_asset;
    private int arg_local_port = -1;
    private String arg_local_hostname;
    private String arg_exchange;
    private String arg_expiry;
    private String arg_symbol;
    private String arg_currency;
    private String arg_primary_exchange;
    private String arg_multiplier;
    private String arg_endDateTime;
    private String arg_durationStr;
    private String arg_barSizeSetting;
    private String arg_outfile;
    private String arg_msgfile;
    private PrintStream outputStream;

    private String getTime() {
	return dateFormat.format(new Date());
    }

    public void setClient(EClientSocket client) {
	m_client = client;
    }

    public EClientSocket getClient() {
	if (m_client == null) {
	    m_client = new EClientSocket(this);
	}
	return m_client;
    }

    public int getClientId() {
	return m_clientId;
    }

    public void setClientId(int id) {
	m_clientId = id;
    }

    public boolean getReconnect() {
	return reconnect;
    }

    public void setReconnect(boolean reconnect) {
	this.reconnect = reconnect;
    }

    protected unicycleWrapper() {
	this(new String[] {});
    }

    protected unicycleWrapper(String args[]) {
	attachDisconnectHook(this);
	parseArgs(args);
// 	initNextOutput();

        try {
	    Reader reader = new Reader(getLocalHostname(), getLocalPort(), getInputStream()) {
		    public void received(String str) {
			message("[Reader.received] "+str);
			if (str.equals("EXIT")) {
			    exit(99);
			}
		    }
		};

	    reader.start();

	} catch( Exception e) {
	    e.printStackTrace();
        }

	message("=================================================================");
	message("Started...");
	connect();
    }

    public void parseArgs(String args[]) {
	int ii = 0;
	while (ii < args.length) {
	    String arg = args[ii];
	    if (arg.equals("-s") || arg.equals("-symbol")) {
		arg_symbol = args[++ii];
	    } else if (arg.equals("-a") || arg.equals("-asset")) {
		arg_asset = args[++ii];
	    } else if (arg.equals("-local_hostname")) {
		arg_local_hostname = args[++ii];
	    } else if (arg.equals("-local_port")) {
		arg_local_port = Integer.valueOf(args[++ii]).intValue();
	    } else if (arg.equals("-end") || arg.equals("-endDateTime")) {
		arg_endDateTime = args[++ii];
	    } else if (arg.equals("-durationStr")) {
		arg_durationStr = args[++ii];
	    } else if (arg.equals("-currency")) {
		arg_currency = args[++ii];
	    } else if (arg.equals("-primary_exchange")) {
		arg_primary_exchange = args[++ii];
	    } else if (arg.equals("-exchange")) {
		arg_exchange = args[++ii];
	    } else if (arg.equals("-expiry")) {
		arg_expiry = args[++ii];
	    } else if (arg.equals("-multiplier")) {
		arg_multiplier = args[++ii];
	    } else if (arg.equals("-barSizeSetting")) {
		arg_barSizeSetting = args[++ii];
	    } else if (arg.equals("-o") || arg.equals("-outfile")) {
		arg_outfile = args[++ii];
	    } else if (arg.equals("-msgfile")) {
		arg_msgfile = args[++ii];
	    }
	    ii++;
	}
    }

    public InputStream getInputStream() throws IOException {
	if (getLocalPort() == -1) {
	    return System.in;
	}
	return null;
    }

    public int getLocalPort() {
	return arg_local_port;
    }

    public String getLocalHostname() {
	if (arg_local_hostname == null) {
	    arg_local_hostname = getString("Local.Hostname");
	}
	return arg_local_hostname;
    }

    public String getSymbol() {
	return arg_symbol;
    }

    public String getOutfile() {
	return arg_outfile;
    }

    public String getMessagefile() {
	return arg_msgfile;
    }

    public PrintStream getOutputStream() {
	if (outputStream == null) {
	    if (getOutfile() == null) {
		outputStream = System.out;
	    } else {
		try {
		    outputStream = new PrintStream(new File(getOutfile()));
		} catch (IOException ioe) {
		    ioe.printStackTrace();
		}		
	    }
	}
	return outputStream;
    }

    public PrintStream getMessageStream() {
	if (messageStream == null) {
	    if (getMessagefile() == null) {
		messageStream = System.out;
	    } else {
		try {
		    messageStream = new PrintStream(new File(getMessagefile()));
		} catch (IOException ioe) {
		    ioe.printStackTrace();
		}		
	    }
	}
	return messageStream;
    }

    public void closeStreams() {
	message("Closing streams.");
	if (messageStream != null) {
	    messageStream.close();
	}
	if (outputStream != null) {
	    outputStream.close();
	}
    }

    public String getEndDateTime() {
	return arg_endDateTime;
    }

    public String getCurrency() {
	if (arg_currency == null) {
	    arg_currency = getString(getVar("Currency"));
	}
	return arg_currency;
    }

    public String getExchange() {
	if (arg_exchange == null) {
	    arg_exchange = getString(getVar("Exchange"));
	}
	return arg_exchange;
    }

    public String getPrimaryExchange() {
	return arg_primary_exchange;
    }

    public String getExpiry() {
	return arg_expiry;
    }

    public String getMultiplier() {
	return arg_multiplier;
    }

    public String getAsset() {
	return arg_asset;
    }

    public String getDurationStr() {
	if (arg_durationStr == null) {
	    arg_durationStr = getString(getVar("DurationStr"));
	}
	return arg_durationStr;
    }

    public String getBarSizeSetting() {
	if (arg_barSizeSetting == null) {
	    arg_barSizeSetting = getString(getVar("BarSizeSetting"));
	}
	return arg_barSizeSetting;
    }

    public void connect() {
	connect(getClientId());
    }

    public void connect(int clientId) {
	setClient(null);
	String host = getResourceBundle().containsKey("Client.Host") ? getString("Client.Host") : "";
	getClient().eConnect(host, getInt("Client.Port"), clientId);	
    }

    public void disconnect() {
	getClient().eDisconnect();
    }

    public Contract getContract() {
	if (contract == null) {
	    contract = new Contract();
	}
	return contract;
    }

    public void returnedErrorCode(int code) {
// 	message("ReturnedErrorCode: "+code);
	errorCode(code);
    }

    public void errorCode(int code) {
	switch (code) {
	case 162:
	case 166:
	case 200:
	case 326:
	case 502:
	case 1100:
	case 2105:
	case 2110:
	    //--------------------------------------------------------------------------------------------------------
	    // ** NOTE: if you add an error code here you must also edit the following:
	    // ./default.properties
	    // ${UNICYCLE_HOME}/lib/mod_tks.py (IB_errors)
	    //
	    // 162: Historical Market Data Service error message: BEST queries are not supported for this contract
	    // 166: HMDS Expired Contract Violation
	    // 200: No security definition has been found for the request
	    // 326: Unable to connect as the client id is already in use. Retry with a unique client id.
	    // 502: Couldn't connect to TWS. Confirm that API is enabled in TWS via the Configure>API menu command.
	    // 1100: Connectivity between IB and TWS has been lost.
	    // 2105: HMDS data farm connection is broken:ushmds2a
	    // 2110: Connectivity between TWS and server is broken. It will be restored automatically.
	    //--------------------------------------------------------------------------------------------------------
	    System.out.println("---------------- ERROR ----------------");
	    exit(getInt(String.format("Error.%d", code)));
	    break;
	    // 		    case 2104:
	    // 		    case 2106:
	    // 			System.out.println("---------------- STATUS: OK ----------------");
	    // 			exit(0);
	    // 			break;
	default:
	    break;
	}
    }

    public void exit(int code) {
	message("exit: " + code);
 	disconnect();
	closeStreams();
	System.exit(code);
    }

    /* ***************************************************************
     * ResourceBundle
     *****************************************************************/

    protected ResourceBundle getResourceBundle() {
	if (bundle == null) {
	    bundle = ResourceBundle.getBundle("Collect.default");
	}
	return bundle;
    }

    public String getString(String key) {
	try {
	    return getResourceBundle().getString(key);
	} catch (MissingResourceException e) {
	    e.printStackTrace();
	}
	return null;
    }

    public char getChar(String key) {
	return (getString(key)).charAt(0);
    }

    public int getInt(String key) {
	return (Integer.parseInt(getString(key)));
    }

    public float getFloat(String key) {
	return (Float.parseFloat(getString(key)));
    }

    /* ***************************************************************
     * AnyWrapper
     *****************************************************************/

    public void error(Exception e) {
	e.printStackTrace(getMessageStream());
    }

    public void error(String str) {
	message(str);
    }

    public void error(int id, int errorCode, String errorMsg) {
	message("Error id=" + id + " code=" + errorCode + " msg=" + errorMsg);
	returnedErrorCode(errorCode);
    }

    public void message(String str) {
	getMessageStream().println(String.format("%s: %s", getTime(), str));
    }	

    public void connectionClosed() {
	message("Connection closed");
	closed();
	if (getReconnect()) {
	    setReconnect(false);
	    connect();
	}
    }	

    public void connectionOpened() {
	message("Connection opened");
	opened();
    }	

    /* ***************************************************************
     * EWrapper
     *****************************************************************/

    public void tickPrice(int tickerId, int field, double price, int canAutoExecute) {
	logIn("tickPrice");
    }

    public void tickSize(int tickerId, int field, int size) {
	logIn("tickSize");
    }

    public void tickGeneric(int tickerId, int tickType, double value) {
	logIn("tickGeneric");
    }

    public void tickString(int tickerId, int tickType, String value) {
	logIn("tickString");
    }	

    public void tickSnapshotEnd(int tickerId) {
	logIn("tickSnapshotEnd");
    }	
   
    public void tickOptionComputation(int tickerId, int field, double impliedVol,
				      double delta, double optPrice, double pvDividend,
				      double gamma, double vega, double theta, double undPrice) {
	logIn("tickOptionComputation");
    }	

    public void tickEFP(int tickerId, int tickType, double basisPoints,
			String formattedBasisPoints, double impliedFuture, int holdDays,
			String futureExpiry, double dividendImpact, double dividendsToExpiry) {
	logIn("tickEFP");
    }

    public void orderStatus(int orderId, String status, int filled, int remaining,
			    double avgFillPrice, int permId, int parentId, double lastFillPrice,
			    int clientId, String whyHeld) {
	logIn("orderStatus");    	
    }

    public void openOrder(int orderId, Contract contract, Order order, OrderState orderState) {
	logIn("openOrder");
    }

    public void openOrderEnd() {
	logIn("openOrderEnd");
    }

    public void updateAccountValue(String key, String value, String currency, String accountName) {
	logIn("updateAccountValue");
    }

    public void updatePortfolio(Contract contract, int position, double marketPrice, double marketValue,
				double averageCost, double unrealizedPNL, double realizedPNL, String accountName) {
	logIn("updatePortfolio");
    }

    public void updateAccountTime(String timeStamp) {
	logIn("updateAccountTime");
    }

    public void accountDownloadEnd(String accountName) {
	logIn("accountDownloadEnd");
    }

    public void nextValidId(int orderId) {
	logIn("nextValidId: "+orderId);
    }

    public void contractDetails(int reqId, ContractDetails contractDetails) {
	logIn("contractDetails");
    }

    public void contractDetailsEnd(int reqId) {
	logIn("contractDetailsEnd");
    }

    public void bondContractDetails(int reqId, ContractDetails contractDetails) {
	logIn("bondContractDetails");
    }

    public void execDetails(int reqId, Contract contract, Execution execution) {
	logIn("execDetails");
    }

    public void execDetailsEnd(int reqId) {
	logIn("execDetailsEnd");
    }

    public void updateMktDepth(int tickerId, int position, int operation, int side, double price, int size) {
	logIn("updateMktDepth");
    }

    public void updateMktDepthL2(int tickerId, int position, String marketMaker, int operation,
				 int side, double price, int size) {
	logIn("updateMktDepthL2");
    }

    public void updateNewsBulletin(int msgId, int msgType, String message, String origExchange) {
	logIn("updateNewsBulletin");
    }

    public void managedAccounts(String accountsList) {
	logIn("managedAccounts");
    }

    public void receiveFA(int faDataType, String xml) {
	logIn("receiveFA");
    }

    public String getFormattedDateStr(String date) {
	return String.format("%s-%s-%s %s", date.substring(0,4), date.substring(4,6), date.substring(6,8), date.substring(date.length()-8));
    }

    public String getVar(String str) {
	return String.format("%s.%s", getAsset(), str);
    }

    public String getSymbolProperName() {
	if (getAsset().equals("fx")) {
	    return String.format("%s%s", getSymbol(), getCurrency());
	} else if (getAsset().equals("futures")) {
	    return String.format("%s%s", getSymbol(), getExpiry());
	}
	return getSymbol();
    }

    public void reqHistoricalData() {

 	getContract().m_symbol = getSymbol();
 	getContract().m_secType = getString(getVar("SecType"));
 	getContract().m_exchange = getExchange();
  	getContract().m_currency = getCurrency();

	if (getAsset().equals("futures")) {
	    getContract().m_expiry = getExpiry();
	    getContract().m_multiplier = getMultiplier();
	    getContract().m_includeExpired = Boolean.parseBoolean(getString(getVar("IncludeExpired")));
	}

	if (getPrimaryExchange() != null) {
	    getContract().m_primaryExch = getPrimaryExchange();
	}

 	message(String.format("[%s] Start collecting %s, endDateTime=%s", "reqHistoricalData", getSymbolProperName(), getEndDateTime()));
 	getClient().reqHistoricalData(getInt(getVar("TickerId")),
				      getContract(),
				      getEndDateTime(),
				      getDurationStr(),
				      getBarSizeSetting(),
				      getString(getVar("WhatToShow")),
				      getInt(getVar("RTH")),
				      getInt("IB.FormatDate"));
    }

    public void historicalData(int reqId, String date, double open, double high, double low,
			       double close, int volume, int count, double WAP, boolean hasGaps) {
	if (header == null) {
	    getOutputStream().println("\"YYYY-MM-DD HH:MM:SS\" Open High Low Close Volume WAP HasGaps Count");
	    header = Boolean.TRUE;
	}

	if (date.contains("finished")) {
	    reqComplete(reqId);
	} else {
	    getOutputStream().println(String.format("\"%s\" %.6f %.6f %.6f %.6f %d %.6f %d %d", getFormattedDateStr(date), open, high, low, close, volume, WAP, hasGaps ? 1 : 0, count));
	}
    }

    public void scannerParameters(String xml) {
	logIn("scannerParameters");
    }

    public void scannerData(int reqId, int rank, ContractDetails contractDetails, String distance,
			    String benchmark, String projection, String legsStr) {
	logIn("scannerData");
    }

    public void scannerDataEnd(int reqId) {
	logIn("scannerDataEnd");
    }

    public void realtimeBar(int reqId, long time, double open, double high, double low, double close, 
			    long volume, double wap, int count) {
	logIn("realtimeBar");
    }

    public void currentTime(long millis) {
	logIn("currentTime");
    }

    public void fundamentalData(int reqId, String data) {
	logIn("fundamentalData");    	
    }

    public void deltaNeutralValidation(int reqId, UnderComp underComp) {
	logIn("deltaNeutralValidation");    	
    }

    /* ***************************************************************
     * Helpers
     *****************************************************************/

    protected void logIn(String method) {
	message("[method]: " + method);
// 	m_messageCounter++;
// 	if (m_messageCounter == MAX_MESSAGES) {
// 	    m_output.close();
// 	    initNextOutput();
// 	    m_messageCounter = 0;
// 	}    	
// // 	m_output.println("[unicycleWrapper] > " + method);
// 	message("[unicycleWrapper] > " + method);
    }

    protected void consoleMsg(String str) {
	message(Thread.currentThread().getName() + " (" + tsStr() + "): " + str);
    }

    protected String tsStr() {
	synchronized (m_df) {
	    return m_df.format(new Date());			
	}
    }

    protected void sleepSec(int sec) {
	sleep(sec * 1000);
    }

    protected void sleep(int msec) {
	try {
	    Thread.sleep(msec);
	} catch (Exception e) { /* noop */ }
    }

    protected void swStart() {
	ts = System.currentTimeMillis();
    }

    protected void swStop() {
	long dt = System.currentTimeMillis() - ts;
	message("[API]" + " Time=" + dt);
    }

//     private void initNextOutput() {
// 	try {
// 	    m_output = new PrintStream(new File("sysout_" + (m_outputCounter++) + ".log"));
// 	} catch (IOException ioe) {
// 	    ioe.printStackTrace();
// 	}		
//     }

    private static void attachDisconnectHook(final unicycleWrapper w) {
	Runtime.getRuntime().addShutdownHook(new Thread() {				
		public void run() {
		    w.message("Shutting down...");
//  		    w.disconnect();
// 		    w.setClient(null);
// 		    w.closeStreams();
		}
	    });			    	
    }    
}
