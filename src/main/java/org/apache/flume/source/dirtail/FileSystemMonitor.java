package org.apache.flume.source.dirtail;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.Validate;
import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemMonitor {

    private static final Logger logger   = LoggerFactory.getLogger(FileSystemMonitor.class);

    private FileSystemManager   fsManager;
    private DefaultFileMonitor  fileMonitor;
    private Set<String>         filesSet = new HashSet<String>();

    public FileSystemMonitor(final DirTailSource source, final DirPattern dirPattern) {
        try {
            this.fsManager = VFS.getManager();
        } catch (FileSystemException e) {
            logger.error("FileSystemException", e);
            Validate.isTrue(false);
        }
        this.fileMonitor = new DefaultFileMonitor(new FileListener() {
            @Override
            public synchronized void fileDeleted(FileChangeEvent event) throws Exception {
                String p = event.getFile().getName().getPath();
                if (filesSet.contains(p)) {
                    filesSet.remove(p);
                    source.removeTask(p);
                }
            }

            @Override
            public synchronized void fileCreated(FileChangeEvent event) throws Exception {
                addJob(event, dirPattern, source, true);
            }

            @Override
            public synchronized void fileChanged(FileChangeEvent event) throws Exception {
                addJob(event, dirPattern, source, false);
            }
        });
        fileMonitor.setRecursive(false);
        try {
            FileObject fileObject = fsManager.resolveFile(dirPattern.getPath());
            if (!fileObject.isReadable() || FileType.FOLDER != fileObject.getType()) {
                logger.warn("Error Path, " + fileObject.getURL());
                Validate.isTrue(false);
            }
            fileMonitor.addFile(fileObject);
            fileMonitor.setDelay(1000);
            fileMonitor.start();
        } catch (FileSystemException e) {
            logger.error("FileSystemException", e);
            Validate.isTrue(false);
        }
    }

    public void stop() {
        this.fileMonitor.stop();
    }

    public void addJob(FileChangeEvent event, DirPattern dirPattern, DirTailSource source, boolean isNew) {
        String p = event.getFile().getName().getPath();
        if (!filesSet.contains(p) && dirPattern.isMatchFile(event)) {
            filesSet.add(p);
            source.commitTask(p, isNew);
        }
    }

}
