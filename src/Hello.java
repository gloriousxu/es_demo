/**
 * Created by xuchao on 2016/12/16.
 */
public class Hello {
    int a;
    public void say(){
        System.out.println("Hello world");
        a=123;
    }

    public static void main(String[] args) {
        System.out.println();
        Hello h = new Hello();
        h.say();
        Hello.InHello ih = h.new InHello();
        ih.inSay();
        h.callback(2);
    }

    class InHello{
        public void inSay(){
            a=1;
            System.out.println(a);
            a=2;
        }
    }

    public void callback(final int a){
        new Worker(){
            public int dosome(){
                return a+2;
            }
        }.dosome();
    }

}

class T{
    public void t(){
        System.out.printf("aaa");
    }
}

