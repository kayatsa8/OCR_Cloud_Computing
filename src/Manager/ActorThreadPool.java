import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ActorThreadPool {

    private final ExecutorService threads;

    public ActorThreadPool(int threads) {
        this.threads = Executors.newFixedThreadPool(threads);
    }

    public void submit(Runnable r) {
        execute(r);
    }

    public void shutdown() {
        threads.shutdownNow();
    }

    private void execute(Runnable r) {
        threads.execute(() -> {
            try {
                r.run();
            }
            catch(Exception e){
                System.out.println(e.getMessage());
            }
        });
    }


}
