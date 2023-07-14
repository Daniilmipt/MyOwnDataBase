package boostbrain;

public enum DataTypes {
    String("java.lang.String"),
    Float("java.lang.Float"),
    Double("java.lang.Double"),
    Byte("java.lang.Byte"),
    Integer("java.lang.Integer"),
    ;
    private final String description;

    DataTypes(String s) {
        this.description = s;
    }

    public String descr(){
        return this.description;
    }
}
