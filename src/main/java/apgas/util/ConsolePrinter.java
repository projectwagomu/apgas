package apgas.util;

import static apgas.Constructs.here;
import static apgas.Constructs.place;

import java.io.Serializable;
import java.net.InetAddress;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import apgas.Configuration;

/**
 * Helper class to avoid mixing outputs from different places and to add a
 * timestamp
 *
 * @author Jonas Posner
 */
public class ConsolePrinter implements Serializable {

	private static final ConsolePrinter instance = new ConsolePrinter();

	/** Stores the state of the {@link Configuration#CONFIG_APGAS_CONSOLEPRINTER} */
	private static final boolean PRINT = Configuration.CONFIG_APGAS_CONSOLEPRINTER.get();

	/** Serial Version UID */
	private static final long serialVersionUID = 6535048433179703185L;

	/** Obtain the printer instance for this process */
	public static synchronized ConsolePrinter getInstance() {
		return ConsolePrinter.instance;
	}

	public synchronized boolean getStatus() {
		return PRINT;
	}

	public synchronized void print(String output) {
		if (PRINT) {
			final String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
			final String time = ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME);
			System.out.print(here() + " [" + time + "] (in " + callerName + ") : " + output);
		}
	}

	public synchronized void printErr(String output) {
		if (PRINT) {
			final String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
			final String time = ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME);
			System.err.print(here() + " [" + time + "] (in " + callerName + ") : " + output);
		}
	}

	public synchronized void println(String output) {
		if (PRINT) {
			final String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
			final String time = ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME);
			String host = "localhost";
			try {
				host = InetAddress.getLocalHost().getHostName();
			} catch (final Exception e) {
				e.printStackTrace();
			}
			System.out.println(here() + "@" + host + " [" + time + "] (in " + callerName + ") : " + output);
		}
	}

	public synchronized void printlnAlways(String output) {
		final String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
		final String time = ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME);
		String host = "localhost";
		try {
			host = InetAddress.getLocalHost().getHostName();
		} catch (final Exception e) {
			e.printStackTrace();
		}
		System.out.println(here() + "@" + host + " [" + time + "] (in " + callerName + ") : " + output);
	}

	public synchronized void printlnErr(String output) {
		if (PRINT) {
			final String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
			final String time = ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME);
			System.err.println(here() + " [" + time + "] (in " + callerName + ") : " + output);
		}
	}

	public synchronized void printlnWithoutAPGAS(String output) {
		if (PRINT) {
			final String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
			final String time = ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME);
			System.out.println("[" + time + "] (in " + callerName + ") : " + output);
		}
	}

	public synchronized void remotePrintln(int source, String output) {
		if (PRINT) {
			final String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
			final String time = ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME);
			System.out
					.println(place(source) + " (in " + callerName + " at " + here().id + ") [" + time + "]: " + output);
		}
	}

	public synchronized void remotePrintlnErr(int source, String output) {
		if (PRINT) {
			final String callerName = Thread.currentThread().getStackTrace()[2].getMethodName();
			final String time = ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME);
			System.err
					.println(place(source) + " (in " + callerName + " at " + here().id + ") [" + time + "]: " + output);
		}
	}
}
