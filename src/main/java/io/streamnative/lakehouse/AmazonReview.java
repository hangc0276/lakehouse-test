package io.streamnative.lakehouse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AmazonReview {
    private String polarity;
    private String title;
    private String text;
}
