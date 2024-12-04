import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {

    public static void main(String[] args) {
        Main main = new Main();
        TaskExecutorImpl taskExecutor = main.new TaskExecutorImpl(5);
        UUID group1 = UUID.randomUUID();
        UUID group2 = UUID.randomUUID();

        TaskGroup taskGroup1 = new TaskGroup(group1);
        TaskGroup taskGroup2 = new TaskGroup(group2);

        Task<Integer> task3 = new Task<>(UUID.randomUUID(), taskGroup2, TaskType.READ, () -> {
            System.out.println("Task 3 executing");
            return 30;
        });

        Task<Integer> task1 = new Task<>(UUID.randomUUID(), taskGroup1, TaskType.READ, () -> {
            System.out.println("Task 1 executing");
            return 10;
        });

        Task<Integer> task2 = new Task<>(UUID.randomUUID(), taskGroup1, TaskType.READ, () -> {
            System.out.println("Task 2 executing");
            return 20;
        });



        Task<Integer> task4 = new Task<>(UUID.randomUUID(), taskGroup2, TaskType.READ, () -> {
            System.out.println("Task 4 executing");
            return 40;
        });

        Task<Integer> task5 = new Task<>(UUID.randomUUID(), taskGroup2, TaskType.READ, () -> {
            System.out.println("Task 5 executing");
            return 50;
        });


        List<Task<Integer>> tasks = Arrays.asList(task1, task2, task3, task4, task5);

        List<CompletableFuture<Integer>> futures = new ArrayList<>();

        for (Task<Integer> task :tasks){
            futures.add((CompletableFuture<Integer>) taskExecutor.submitTask(task));
        }

        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            combinedFuture.join();
        } catch (final Exception e) {
            throw e;
        }finally {
            taskExecutor.shutdown();
        }



    }
    /**
     * Enumeration of task types.
     */
    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {
        /**
         * Submit new task to be queued and executed.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @return Future for the task asynchronous computation result.
         */
        <T> Future<T> submitTask(Task<T> task);
    }

    public class TaskExecutorImpl implements TaskExecutor {

        private final int maxAllowedConcurrency;

        private final HashMap<String, CompletableFuture> futureMap;

        private final ExecutorService executorService;

        public TaskExecutorImpl(int maxAllowedConcurrency) {
            this.maxAllowedConcurrency = maxAllowedConcurrency;
            this.futureMap = new HashMap<>();
             executorService = Executors.newFixedThreadPool(maxAllowedConcurrency);
        }


        @Override
        public <T> Future<T> submitTask(Task<T> task) {

            CompletableFuture<T> lastFuture = getFutureMapForTaskGroup(task.taskGroup);

            CompletableFuture<T> taskFuture = (lastFuture != null)
                    ? ((CompletableFuture<T>) lastFuture).thenCompose(v -> executeTask(task, executorService))
                    : executeTask(task, executorService);


            futureMap.put(task.taskUUID.toString(), taskFuture);

            return taskFuture;

        }

        private <T> CompletableFuture<T> getFutureMapForTaskGroup(TaskGroup taskGroup) {
            return futureMap.get(taskGroup.groupUUID.toString());
        }

        private <T> CompletableFuture<T> executeTask(Task<T> task, ExecutorService executorService) {
            return CompletableFuture.supplyAsync(() -> {
                try {
// Execute the callable task
                    return task.taskAction().call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executorService).exceptionally(e->{
                System.err.println("Error in task :" + e.getMessage());
                return null;
            });
        }

        public void shutdown(){
            executorService.shutdown();
        }
    }

    /**
     * Representation of computation to be performed by the {@link TaskExecutor}.
     *
     * @param taskUUID Unique task identifier.
     * @param taskGroup Task group.
     * @param taskType Task type.
     * @param taskAction Callable representing task computation and returning the result.
     * @param <T> Task computation result value type.
     */
    public record Task<T>(
            UUID taskUUID,
            TaskGroup taskGroup,
            TaskType taskType,
            Callable<T> taskAction
    ) {
        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    /**
     * Task group.
     *
     * @param groupUUID Unique group identifier.
     */
    public record TaskGroup(
            UUID groupUUID
    ) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

}
