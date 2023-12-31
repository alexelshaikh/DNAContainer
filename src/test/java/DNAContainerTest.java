import datastructures.container.DNAContainer;
import datastructures.container.impl.RichDNAContainer;
import datastructures.container.translation.DNAAddrManager;
import datastructures.hashtable.BloomFilter;
import dnacoders.dnaconvertors.*;
import packaging.Application;
import utils.Coder;
import utils.lsh.minhash.MinHashLSH;
import utils.rq.RQCoder;
import java.util.Arrays;

public class DNAContainerTest {

    public static void main(String[] args) {
        int r = 5;
        int k = 6;
        int numElements = 10;
        double eps = 0.01d;
        long nBits = BloomFilter.numBits(eps, numElements);
        long nHashFunctions = BloomFilter.numHashFunctions(eps);

        int addressSize = 80;
        int payloadSize = 170;
        int addressPermutations = 8;
        int payloadPaddingSize = 10;
        int payloadPermutations = 8;


        var coderGoldman = RotatingTre.INSTANCE;
        var coderQuattro = RotatingQuattro.INSTANCE;
        var coderBin = Bin.INSTANCE;
        var coderFountain = Coder.fuse(Application.BIJECTIVE_STRING_CODER, RQCoder.DEFAULT_RQ); // fountain code

        //var coder = coderGoldman;
        //var coder = coderQuattro;
        //var coder = coderBin;
        var coder = coderFountain;

        DNAAddrManager atm = DNAAddrManager
                .builder()
                .setLsh(MinHashLSH.newSeqLSHBloom(k, r, nBits, nHashFunctions))
                .setAddrSize(addressSize)
                .setNumPermutations(addressPermutations)
                .build();


        RichDNAContainer<String> container = DNAContainer
                .builder()
                .setAddressManager(atm)
                .setOligoLSH(MinHashLSH.newSeqLSHBloom(k, r, nBits, nHashFunctions))
                .setPayloadSize(payloadSize)
                .setNumGcCorrectionsPayload(payloadPaddingSize)
                .setNumPayloadPermutations(payloadPermutations)
                .buildToRichContainer(coder);



        // reference
        long referenceId = container.registerId();
        var ref = container.putReference(referenceId, "hello world");
        System.out.println("getting the reference with id: " + referenceId + " -> " + container.getReference(referenceId).decode());
        System.out.println("getting the reference by ref.decode(): " + " -> " + ref.decode());

        System.out.println();

        // array
        var array = container.putArray(new String[] {"hello world 1", "hello world 2", "hello world 3"});
        System.out.println("getting the elements of the array with id: " + array.sketch().id() + " -> " + container.getArray(array.sketch().id()).decode());
        System.out.println("getting the elements of the array by array.decode(): " + " -> " + array.decode());

        // number of objects in the array
        int arrayLength = array.length();
        for (int i = 0; i < arrayLength; i++)
            System.out.println("\tpos: " + i + " -> " + array.get(i));

        System.out.println();

        // list
        var list = container.putList(Arrays.asList("hello world 1", "hello world 2", "hello world 3"));
        System.out.println("getting the elements of the list with id: " + list.sketch().id() + " -> " + container.getList(list.sketch().id()).decode());
        System.out.println("getting the elements of the list by list.decode(): " + " -> " + list.decode());

        System.out.println();

        // appending to the list
        System.out.println("successfully appended a new element to the list: " + list.append("hello world 4"));
        System.out.println(list.decode());

        System.out.println();

        // inserting a new element into an arbitrary list's position
        int pos = 0;
        System.out.println("inserting a new element into the list at position " + pos);
        list.insert(pos, "hello new element!");
        System.out.println("getting the elements of the list by list.decode(): " + " -> " + list.decode());
    }
}
