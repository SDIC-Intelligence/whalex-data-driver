package com.meiya.whalex.filesystem.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件树
 *
 * @author 黄河森
 * @date 2023/9/1
 * @package com.meiya.whalex.filesystem.entity
 * @project whalex-data-driver
 * @description FileTree
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileTreeNode implements Serializable {

    /**
     * 路径或者文件名称
     */
    private String name;

    /**
     * 路径下的子目录
     */
    private List<FileTreeNode> children;

    /**
     * 文件大小
     */
    private long length;

    /**
     * 修改时间
     */
    private long modificationTime;

    /**
     * 是否是目录
     */
    private boolean isDir;

    /**
     * 是否是文件
     */
    private boolean isFile;

    /**
     * 目录下文件总数
     */
    private Long total;

    /**
     * 命中文件数
     */
    private Long hitTotal;

    public static FileTreeNodeBuilder builder() {
        return new FileTreeNodeBuilder();
    }

    public static class FileTreeNodeBuilder {
        private FileTreeNode fileTreeNode;

        private FileTreeNodeBuilder() {
            this.fileTreeNode = new FileTreeNode();
        }

        public FileTreeNode build() {
            return fileTreeNode;
        }

        public FileTreeNodeBuilder name(String name) {
            this.fileTreeNode.setName(name);
            return this;
        }

        public FileTreeNodeBuilder length(long length) {
            this.fileTreeNode.setLength(length);
            return this;
        }

        public FileTreeNodeBuilder modificationTime(long modificationTime) {
            this.fileTreeNode.setModificationTime(modificationTime);
            return this;
        }

        public FileTreeNodeBuilder isDir(boolean isDir) {
            this.fileTreeNode.setDir(isDir);
            return this;
        }

        public FileTreeNodeBuilder isFile(boolean isFile) {
            this.fileTreeNode.setFile(isFile);
            return this;
        }

        public FileTreeNodeBuilder total(long total) {
            this.fileTreeNode.setTotal(total);
            return this;
        }

        public FileTreeNodeBuilder hitTotal(long hitTotal) {
            this.fileTreeNode.setHitTotal(hitTotal);
            return this;
        }

        public FileTreeNodeBuilder children(FileTreeNode fileTreeNode) {
            List<FileTreeNode> children = this.fileTreeNode.getChildren();
            if (children == null) {
                children = new ArrayList<>();
                this.fileTreeNode.setChildren(children);
            }
            children.add(fileTreeNode);
            return this;
        }

        public FileTreeNodeBuilder children(List<FileTreeNode> fileTreeNode) {
            List<FileTreeNode> children = this.fileTreeNode.getChildren();
            if (children == null) {
                children = new ArrayList<>();
                this.fileTreeNode.setChildren(children);
            }
            children.addAll(fileTreeNode);
            return this;
        }

    }

}
