import java.util.BitSet;

public class BloomFillter {

    /**
     * bitSet的大小
     */
    private static final int DEFAULT_SIZE = 2 << 24;
    /**
     * 选取的hash函数
     */
    /**
     * bitSet每一位只能是true或false  其实就是bit数组说的0或者1
     */
    private static final int[] SEEDS = new int[]{3, 13, 46, 71, 91, 134};
    private BitSet bits = new BitSet(DEFAULT_SIZE);
    private SimpleHash[] func = new SimpleHash[SEEDS.length];

    public BloomFillter() {
        for (int i = 0; i < SEEDS.length; i++) {
            func[i] = new SimpleHash(DEFAULT_SIZE, SEEDS[i]);
        }
    }

    public static void main(String[] args) {
        String value = "wxwwt@gmail.com";
        BloomFillter filter = new BloomFillter();
        System.out.println(filter.contains(value));
        filter.add(value);

        System.out.println(filter.contains(value));
    }



    public void add(String value) {
        for (SimpleHash f : func) {

            bits.set(f.hash(value), true);
        }
    }

    public boolean contains(String value) {
        if (value == null) {
            return false;
        }
        boolean ret = true;
        for (SimpleHash f : func) {
            ret = ret && bits.get(f.hash(value));
        }
        return ret;
    }
}

class SimpleHash {

    private int cap;
    private int seed;

    public SimpleHash(int cap, int seed) {
        this.cap = cap;
        this.seed = seed;
    }

    /**
     * 计算hash值
     *
     * @param  value of hash
     * @return return hash of value
     */

    public int hash(String value) {
        int result = 0;
        int len = value.length();
        for (int i = 0; i < len; i++) {
            result = seed * result + value.charAt(i);
        }
        return (cap - 1) & result;
    }

}

