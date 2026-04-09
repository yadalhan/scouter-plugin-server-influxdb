package scouter.plugin.server.influxdb;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import scouter.lang.TimeTypeEnum;
import scouter.lang.pack.PerfCounterPack;
import scouter.lang.plugin.PluginConstants;
import scouter.lang.plugin.annotation.ServerPlugin;
import scouter.lang.value.Value;
import scouter.server.ConfObserver;
import scouter.server.Configure;
import scouter.server.CounterManager;
import scouter.server.Logger;
import scouter.server.core.AgentManager;
import scouter.util.HashUtil;

import java.time.Instant;
import java.util.Map;

/**
 * @author Gun Lee (gunlee01@gmail.com) on 2016. 3. 29.
 * Migrated to InfluxDB 3.0 Java Client on 2026
 */
public class InfluxdbPlugin {
    Configure conf = Configure.getInstance();

    private static final String ext_plugin_influxdb_enabled = "ext_plugin_influxdb_enabled";
    private static final String ext_plugin_influxdb_measurement = "ext_plugin_influxdb_measurement";

    private static final String ext_plugin_influxdb_udp = "ext_plugin_influxdb_udp";
    private static final String ext_plugin_influxdb_udp_local_ip = "ext_plugin_influxdb_udp_local_ip";
    private static final String ext_plugin_influxdb_udp_local_port = "ext_plugin_influxdb_udp_local_port";

    private static final String ext_plugin_influxdb_udp_target_ip = "ext_plugin_influxdb_udp_target_ip";
    private static final String ext_plugin_influxdb_udp_target_port = "ext_plugin_influxdb_udp_target_port";

    // InfluxDB 3.0 Configurations
    private static final String ext_plugin_influxdb_http_url = "ext_plugin_influxdb_http_url";
    private static final String ext_plugin_influxdb_token = "ext_plugin_influxdb_token";
    private static final String ext_plugin_influxdb_database = "ext_plugin_influxdb_database";

    private static final String tagObjName = "obj";
    private static final String tagTimeTypeName = "timeType";
    private static final String tagObjType = "objType";
    private static final String tagObjFamily = "objFamily";

    boolean enabled = conf.getBoolean(ext_plugin_influxdb_enabled, true);

    private String measurementName = conf.getValue(ext_plugin_influxdb_measurement, "counter");

    // UDP (Legacy mode configs)
    boolean isUdp = conf.getBoolean(ext_plugin_influxdb_udp, false);
    String udpLocalIp = conf.getValue(ext_plugin_influxdb_udp_local_ip);
    int udpLocalPort = conf.getInt(ext_plugin_influxdb_udp_local_port, 0);
    String udpTargetIp = conf.getValue(ext_plugin_influxdb_udp_target_ip, "127.0.0.1");
    int udpTargetPort = conf.getInt(ext_plugin_influxdb_udp_target_port, 8089);

    UdpAgent udpAgent = null;

    // HTTP / v3 mode configs
    String httpUrl = conf.getValue(ext_plugin_influxdb_http_url, "http://127.0.0.1:8086");
    String token = conf.getValue(ext_plugin_influxdb_token, "");
    String database = conf.getValue(ext_plugin_influxdb_database, "scouterCounter");

    InfluxDBClient influx = null;

    public InfluxdbPlugin() {
        if (isUdp) {
            udpAgent = UdpAgent.getInstance();
            udpAgent.setLocalAddr(udpLocalIp, udpLocalPort);
            udpAgent.setTarget(udpTargetIp, udpTargetPort);
        } else {
            try {
                influx = InfluxDBClient.getInstance(httpUrl, token.toCharArray(), database);
            } catch (Exception e) {
                Logger.println("IFLX000", "Failed to init InfluxDBClient: " + e.getMessage());
            }
        }

        ConfObserver.put("InfluxdbPlugin", new Runnable() {
            public void run() {
                enabled = conf.getBoolean(ext_plugin_influxdb_enabled, true);
                measurementName = conf.getValue(ext_plugin_influxdb_measurement, "counter");
                boolean isUdpNew = conf.getBoolean(ext_plugin_influxdb_udp, false);
                if (isUdpNew != isUdp) {
                    isUdp = isUdpNew;
                    if (isUdp) {
                        udpAgent = UdpAgent.getInstance();
                        udpAgent.setLocalAddr(udpLocalIp, udpLocalPort);
                        udpAgent.setTarget(udpTargetIp, udpTargetPort);
                    } else {
                        try {
                            if(influx != null) influx.close();
                            influx = InfluxDBClient.getInstance(httpUrl, token.toCharArray(), database);
                        } catch(Exception e) {
                             Logger.println("IFLX000", "Failed to init InfluxDBClient: " + e.getMessage());
                        }
                    }
                }

                //set udp local
                String newUdpLocalIp = conf.getValue(ext_plugin_influxdb_udp_local_ip);
                int newUdpLocalPort = conf.getInt(ext_plugin_influxdb_udp_local_port, 0);
                if ((newUdpLocalIp != null && !newUdpLocalIp.equals(udpLocalIp)) || newUdpLocalPort != udpLocalPort) {
                    udpLocalIp = newUdpLocalIp;
                    udpLocalPort = newUdpLocalPort;
                    if(udpAgent != null) udpAgent.setLocalAddr(udpLocalIp, udpLocalPort);
                }

                //set udp target
                String newUdpTargetIp = conf.getValue(ext_plugin_influxdb_udp_target_ip, "127.0.0.1");
                int newUdpTargetPort = conf.getInt(ext_plugin_influxdb_udp_target_port, 8089);
                if (!newUdpTargetIp.equals(udpTargetIp) || newUdpTargetPort != udpTargetPort) {
                    udpTargetIp = newUdpTargetIp;
                    udpTargetPort = newUdpTargetPort;
                    if(udpAgent != null) udpAgent.setTarget(udpTargetIp, udpTargetPort);
                }

                //set http v3 target
                String newHttpUrl = conf.getValue(ext_plugin_influxdb_http_url, "http://127.0.0.1:8086");
                String newToken = conf.getValue(ext_plugin_influxdb_token, "");
                String newDatabase = conf.getValue(ext_plugin_influxdb_database, "scouterCounter");

                if (!newHttpUrl.equals(httpUrl) || !newToken.equals(token) || !newDatabase.equals(database)) {
                    httpUrl = newHttpUrl;
                    token = newToken;
                    database = newDatabase;
                    if(!isUdp) {
                        try {
                            if(influx != null) influx.close();
                            influx = InfluxDBClient.getInstance(httpUrl, token.toCharArray(), database);
                        } catch(Exception e) {
                            Logger.println("IFLX000", "Failed to init InfluxDBClient: " + e.getMessage());
                        }
                    }
                }
            }
        });
    }

    @ServerPlugin(PluginConstants.PLUGIN_SERVER_COUNTER)
    public void counter(final PerfCounterPack pack) {
        if (!enabled) {
            return;
        }

        if(pack.timetype != TimeTypeEnum.REALTIME) {
            return;
        }

        try {
            String objName = pack.objName;
            int objHash = HashUtil.hash(objName);
            String objType = AgentManager.getAgent(objHash).objType;
            String objFamily = CounterManager.getInstance().getCounterEngine().getObjectType(objType).getFamily().getName();
            
            // InfluxDB 3.0 Point Builder
            Point point = Point.measurement(measurementName)
                    .setTimestamp(Instant.ofEpochMilli(pack.time))
                    .setTag(tagObjName, objName)
                    .setTag(tagObjType, objType)
                    .setTag(tagObjFamily, objFamily);

            Map<String, Value> dataMap = pack.data.toMap();
            for (Map.Entry<String, Value> field : dataMap.entrySet()) {
                Value valueOrigin = field.getValue();
                if (valueOrigin == null) {
                    continue;
                }
                Object value = valueOrigin.toJavaObject();
                if(!(value instanceof Number)) {
                    continue;
                }
                String key = field.getKey();
                if("time".equals(key)) {
                    continue;
                }
                
                if(value instanceof Float || value instanceof Double) {
                    point.setField(key, ((Number)value).doubleValue());
                } else if(value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
                    point.setField(key, ((Number)value).longValue());
                } else {
                    point.setField(key, ((Number)value).doubleValue());
                }
            }

            if (isUdp) {
                String line = point.toLineProtocol();
                if(line != null && udpAgent != null) {
                    udpAgent.write(line);
                }
            } else { // http v3
                if(influx != null) {
                    influx.writePoint(point);
                }
            }

        } catch (Exception e) {
            if (conf._trace) {
                Logger.printStackTrace("IFLX001", e);
            } else {
                Logger.println("IFLX002", e.getMessage());
            }
        }
    }
}
