package com.meiya.whalex.filesystem.entity;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.InputStream;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UploadFileParam {

    private InputStream inputStream;
    private String path;
}
