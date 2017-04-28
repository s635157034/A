package AdaBoost;

public class ClassiferInfo {
    private String name;
    private double error;

    public ClassiferInfo(String name, double error) {
        this.name = name;
        this.error = error;
    }

    @Override
    public String toString() {
        return name+":"+String.valueOf(error);
    }
}
