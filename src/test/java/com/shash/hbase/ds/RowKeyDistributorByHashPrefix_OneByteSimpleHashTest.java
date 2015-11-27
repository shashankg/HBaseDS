package com.shash.hbase.ds;

public class RowKeyDistributorByHashPrefix_OneByteSimpleHashTest extends RowKeyDistributorTestBase {

    public RowKeyDistributorByHashPrefix_OneByteSimpleHashTest() {
        super(new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(15)));
    }
}
