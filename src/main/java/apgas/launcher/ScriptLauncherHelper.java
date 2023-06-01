package apgas.launcher;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import apgas.Configuration;

public class ScriptLauncherHelper {

	private static void writeFile(String filename, List<String> commands) throws IOException {
		final BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true));

		writer.append("#!/bin/bash");
		writer.append("\n");
		writer.append("ulimit -u 131064");
		writer.append("\n");
		writer.append("ulimit -s 8192");
		writer.append("\n");
		writer.append("ulimit -i 103061");
		writer.append("\n");

		for (final String c : commands) {
			writer.append(c + " &");
			writer.append("\n");
		}

		writer.append("\n");
		writer.append("FOO_PID=$!");
		writer.append("\n");
		writer.append("wait $FOO_PID");
		writer.append("\n");
		writer.close();
	}

	static void writeFile(String filename, List<String> command, int n, List<String> hosts, List<Integer> newPlaceIDs)
			throws IOException {

		final List<String> allCommands = new ArrayList<>();
		final String remove = command.remove(command.size() - 1);

		boolean localhost = true;
		for (final String h : hosts) {

			int iterN = n;
			if (localhost) {
				iterN--;
				localhost = false;
			}

			for (int i = 0; i < iterN; i++) {

				command.add("-D" + Configuration.CONFIG_APGAS_PLACE_ID.getName() + "=" + newPlaceIDs.remove(0));
				command.add(remove);

				allCommands.add(String.join(" ", command));

				command.remove(command.size() - 1);
				command.remove(command.size() - 1);
			}

			final String iterFileName = filename + "-" + h + ".sh";
			writeFile(iterFileName, allCommands);

			allCommands.clear();
		}
	}
}
