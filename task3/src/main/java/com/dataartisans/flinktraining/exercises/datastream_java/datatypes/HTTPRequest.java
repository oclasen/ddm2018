/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.datastream_java.datatypes;

import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;

/**
 * A TaxiRide is a taxi ride event. There are two types of events, a taxi ride start event and a
 * taxi ride end event. The isStart flag specifies the type of the event.
 *
 * A TaxiRide consists of
 * - the rideId of the event which is identical for start and end record
 * - the type of the event (start or end)
 * - the time of the event
 * - the longitude of the start location
 * - the latitude of the start location
 * - the longitude of the end location
 * - the latitude of the end location
 * - the passengerCnt of the ride
 * - the taxiId
 * - the driverId
 *
 */
public class HTTPRequest implements Serializable {
    
    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("dd/M/yyyy:HH:mm:ss Z").withLocale(Locale.US).withZoneUTC();
    
    public HTTPRequest() {
        this.timeStamp = new DateTime();
    }
    
    public HTTPRequest(String hostString, DateTime timestampDate, String requestString, int replyCodeInt, int byteNumberInt) {
        
        this.host = hostString;
        this.request = requestString;
        this.timeStamp = timestampDate;
        this.replyCode= replyCodeInt;
        this.byteNumber= byteNumberInt;
        
    }
    
    public String host;
    public String request;
    public DateTime timeStamp;
    public int replyCode;
    public int byteNumber;
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(host).append(",");
        sb.append(timeStamp.toString(timeFormatter)).append(",");
        sb.append(request).append(",");
        sb.append(replyCode).append(",");
        sb.append(byteNumber);
        
        return sb.toString();
    }
    
    public static HTTPRequest fromString(String line) {
    
        HTTPRequest httpRequest = new HTTPRequest();
        
        String[] tokens = line.split("[\\w]+([\\.-][\\w]+)*|\\[[\\w\\/:-]+\\]|\"[\\w\\.\\/ ]+\"");
        if (tokens.length != 5) {
            if (tokens.length == 4){
                try {
                    httpRequest.host = tokens[0];
                    httpRequest.timeStamp = DateTime.parse(tokens[1], timeFormatter);
                    httpRequest.request = tokens[2];
                    httpRequest.replyCode = Integer.parseInt(tokens[3]);
                    httpRequest.byteNumber = 0;
                    
                    return httpRequest;
                } catch (NumberFormatException e){
                    throw new RuntimeException("Invalid record: "+ line, e);
                }
            }
            throw new RuntimeException("Invalid record: " + line);
        } else {
    
            try {
                httpRequest.host = tokens[0];
                httpRequest.timeStamp = DateTime.parse(tokens[1], timeFormatter);
                httpRequest.request = tokens[2];
                httpRequest.replyCode = Integer.parseInt(tokens[3]);
                httpRequest.byteNumber = Integer.parseInt(tokens[4]);
        
            } catch (NumberFormatException nfe) {
                throw new RuntimeException("Invalid record: " + line, nfe);
            }
    
            return httpRequest;
        }
    }
    
    // sort by timestamp,
    // putting START events before END events if they have the same timestamp
    public int compareTo(HTTPRequest other) {
        if (other == null) {
            return 1;
        }
        int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
        return compareTimes;
    }
    
    @Override
    public boolean equals(Object other) {
        return other instanceof HTTPRequest &&
                this.toString() == ((HTTPRequest) other).toString();
    }
    
    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
    
    public long getEventTime() {
        return timeStamp.getMillis();
    }
}
