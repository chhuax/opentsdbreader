package com.yonyou.iot.opentsdb.reader.conn;

import net.opentsdb.core.*;
import net.opentsdb.utils.DateTime;

import java.util.ArrayList;
import java.util.HashMap;

final class CliQuery {
    private CliQuery(){}

    /**
     * Parses the query from the command lines.
     *
     * @param args    The command line arguments.
     * @param tsdb    The TSDB to use.
     * @param queries The list in which {@link Query}s will be appended.
     */
    static void parseCommandLineQuery(final String[] args,
                                      final TSDB tsdb,
                                      final ArrayList<Query> queries) {
        long startTs = DateTime.parseDateTimeString(args[0], null);
        if (startTs >= 0) {
            startTs /= 1000;
        }
        long endTs = -1;
        if (args.length > 3) {
            // see if we can detect an end time
            try {
                if (args[1].charAt(0) != '+' && (args[1].indexOf(':') >= 0
                        || args[1].indexOf('/') >= 0 || args[1].indexOf('-') >= 0
                        || Long.parseLong(args[1]) > 0)) {
                    endTs = DateTime.parseDateTimeString(args[1], null);
                }
            } catch (NumberFormatException ignore) {
                // ignore it as it means the third parameter is likely the aggregator
            }
        }
        // temp fixup to seconds from ms until the rest of TSDB supports ms
        // Note you can't append this to the DateTime.parseDateTimeString() call as
        // it clobbers -1 results
        if (endTs >= 0) {
            endTs /= 1000;
        }

        int i = endTs < 0 ? 1 : 2;
        while (i < args.length && args[i].charAt(0) == '+') {
            i++;
        }

        while (i < args.length) {
            final Aggregator agg = Aggregators.get(args[i++]);
            final boolean rate = "rate".equals(args[i]);
            RateOptions rateOptions = new RateOptions(false, Long.MAX_VALUE,
                    RateOptions.DEFAULT_RESET_VALUE);
            if (rate) {
                i++;

                long counterMax = Long.MAX_VALUE;
                long resetValue = RateOptions.DEFAULT_RESET_VALUE;
                if (args[i].startsWith("counter")) {
                    String[] parts = Tags.splitString(args[i], ',');
                    if (parts.length >= 2 && parts[1].length() > 0) {
                        counterMax = Long.parseLong(parts[1]);
                    }
                    if (parts.length >= 3 && parts[2].length() > 0) {
                        resetValue = Long.parseLong(parts[2]);
                    }
                    rateOptions = new RateOptions(true, counterMax, resetValue);
                    i++;
                }
            }
            final boolean downsample = "downsample".equals(args[i]);
            if (downsample) {
                i++;
            }
            final long interval = downsample ? Long.parseLong(args[i++]) : 0;
            final Aggregator sampler = downsample ? Aggregators.get(args[i++]) : null;
            final String metric = args[i++];
            final HashMap<String, String> tags = new HashMap<String, String>();
            while (i < args.length && args[i].indexOf(' ', 1) < 0
                    && args[i].indexOf('=', 1) > 0) {
                Tags.parse(tags, args[i++]);
            }
            final Query query = tsdb.newQuery();
            query.setStartTime(startTs);
            if (endTs > 0) {
                query.setEndTime(endTs);
            }
            query.setTimeSeries(metric, tags, agg, rate, rateOptions);
            if (downsample) {
                query.downsample(interval, sampler);
            }
            queries.add(query);
        }
    }
}
