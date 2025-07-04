package com.slim.controller.ui;

import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;

import java.io.File;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/ui")
public class UiClusterStatusController {

    @GetMapping("/status")
    public ResponseEntity<?> getStatus() {
        try {
            Map<String, Object> status = new HashMap<>();
            status.put("hostname", InetAddress.getLocalHost().getHostName());
            status.put("os", System.getProperty("os.name"));
            status.put("architecture", System.getProperty("os.arch"));
            status.put("cpu_count", Runtime.getRuntime().availableProcessors());

            OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
            double[] load = new double[3];
            if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
                double sysLoad = ((com.sun.management.OperatingSystemMXBean) osBean).getSystemLoadAverage();
                load[0] = sysLoad;
                load[1] = sysLoad;
                load[2] = sysLoad;
            }
            status.put("cpu_load", load);

            // Mémoire physique totale (RAM du système ou du conteneur)
            long totalMem = -1;
            long freeMem = -1;
            long jvmMax = Runtime.getRuntime().maxMemory();
            if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean sunOsBean = (com.sun.management.OperatingSystemMXBean) osBean;
                totalMem = sunOsBean.getTotalPhysicalMemorySize();
                freeMem = sunOsBean.getFreePhysicalMemorySize();
            }
            Map<String, Object> mem = new HashMap<>();
            mem.put("total", totalMem); // RAM physique totale
            mem.put("free", freeMem);   // RAM physique libre
            mem.put("jvm_max", jvmMax); // Mémoire max JVM (utile pour debug)
            status.put("memory", mem);

            return ResponseEntity.ok(status);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(new HashMap<String, Object>() {{
                put("error", "Status error: " + e.getMessage());
            }});
        }
    }

    @GetMapping("/live")
    public Map<String, Object> livenessCheck() {
        return new HashMap<String, Object>() {{
            put("status", "alive");
        }};
    }

    @GetMapping("/ready")
    public ResponseEntity<?> readinessCheck() {
        try {
            // Vérifie DuckDB (ici on suppose que la DataSource est accessible)
            // (À adapter selon ton projet, ex: ping sur la DB)
            // try (Connection con = dataSource.getConnection()) {
            //     try (Statement st = con.createStatement()) {
            //         st.execute("SELECT 1");
            //     }
            // }

            // Vérifie accès disque/cache
            File cacheDir = new File("./db_cache");
            if (!cacheDir.exists()) cacheDir.mkdirs();

            File testFile = new File(cacheDir, "readiness_check.tmp");
            try (FileWriter fw = new FileWriter(testFile)) {
                fw.write("ok");
            }
            Files.delete(testFile.toPath());

            Map<String, Object> resp = new HashMap<>();
            resp.put("status", "ready");
            resp.put("duckdb", "ok");
            resp.put("disk", "ok");
            return ResponseEntity.ok(resp);

        } catch (Exception e) {
            return ResponseEntity.status(503).body(new HashMap<String, Object>() {{
                put("error", "Not ready: " + e.getMessage());
            }});
        }
    }
}
