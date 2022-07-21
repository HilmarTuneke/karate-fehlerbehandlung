public class Controller {
    public void process(String payload) {
    }

    public boolean shouldSkip(String key) {
        return false;
    }

    public void addKeyToSkip(String key) {
    }

    public boolean shouldSkip(int partition, long offset) {
        return false;
    }

    public void notifySkip(int partition, long offset) {
    }
}

