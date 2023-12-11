import javax.xml.crypto.Data;
import java.util.Arrays;

public enum DataType {
    INT(0x06),
    TINYINT(0x04),
    SMALLINT(0x05),
    BIGINT(0x07),
    REAL(0x08),
    DOUBLE(0x09),
    DATETIME(0x0A),
    DATE(0x0B),
    TEXT(0x0C);

    private final int serialCode;

    DataType(int serialCode) {
        this.serialCode = serialCode;
    }

    public int getSerialCode() {
        return serialCode;
    }

   static int valueByName(String dataType) {

        DataType dataType1 = Arrays.stream(DataType.values()).filter(data -> data.name().equalsIgnoreCase(dataType)).findFirst().orElse(null);
        if (dataType1 != null) {
            return dataType1.getSerialCode();
        }
        return 0;
    }
}
