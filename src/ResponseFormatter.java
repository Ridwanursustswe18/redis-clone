class ResponseFormatter {

    public static void printResponse(Object response) {
        if (response == null) {
            System.out.println("(nil)");
        } else if (response instanceof Object[]) {
            Object[] array = (Object[]) response;
            System.out.println("Array[" + array.length + "]:");
            for (int i = 0; i < array.length; i++) {
                System.out.println("  " + i + ") " + array[i]);
            }
        } else {
            System.out.println(response);
        }
    }
}