import core.BaseSequence;
import datastructures.container.Container;
import datastructures.container.DNAContainer;
import datastructures.container.translation.DNAAddrManager;
import datastructures.container.utils.DNAContainerUtils;
import datastructures.hashtable.BloomFilter;
import packaging.Application;
import utils.Coder;
import utils.lsh.minhash.MinHashLSH;
import utils.rq.RQCoder;
import java.util.Objects;

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


        DNAAddrManager atm = DNAAddrManager
                .builder()
                .setLsh(MinHashLSH.newSeqLSHBloom(k, r, nBits, nHashFunctions))
                .setAddrSize(addressSize)
                .setNumPermutations(addressPermutations)
                .build();


        DNAContainer seqContainer = DNAContainer
                .builder()
                .setAddressManager(atm)
                .setOligoLSH(MinHashLSH.newSeqLSHBloom(k, r, nBits, nHashFunctions))
                .setPayloadSize(payloadSize)
                .setNumGcCorrectionsPayload(payloadPaddingSize)
                .setNumPayloadPermutations(payloadPermutations)
                .build();

        //var coder = RotatingTre.INSTANCE;
        //var coder = RotatingQuattro.INSTANCE;
        //var coder = Bin.INSTANCE;
        var coder = Coder.fuse(Application.BIJECTIVE_STRING_CODER, RQCoder.DEFAULT_RQ); // fountain code

        Container<Long, String> container = Container.transform(seqContainer, coder);

        // reference
        long referenceId = seqContainer.registerId();
        container.put(0L, "hello world");
        System.out.println("getting the reference with id: " + referenceId + " -> " + container.get(referenceId));

        System.out.println();

        // array
        long arrayId = DNAContainerUtils.putArray(seqContainer, coder.encode("hello world 1"), coder.encode("hello world 2"), coder.encode("hello world 3"));
        System.out.println("getting the elements of the array of id: " + arrayId);

        // number of objects in the array
        int arrayLength = DNAContainerUtils.getArrayLength(seqContainer, arrayId);
        for (int i = 0; i < arrayLength; i++)
            System.out.println("\tpos: " + i + " -> " + coder.decode(Objects.requireNonNull(DNAContainerUtils.getArrayPos(seqContainer, arrayId, i))));

        System.out.println();

        // list
        long listId = DNAContainerUtils.putList(seqContainer, coder.encode("hello world 1"), coder.encode("hello world 2"), coder.encode("hello world 3"));
        printList(seqContainer, listId, coder);

        System.out.println();

        // appending to the list
        System.out.println("successfully appended a new element to the list: " + DNAContainerUtils.appendToList(seqContainer, listId, coder.encode("hello world 4")));
        printList(seqContainer, listId, coder);

        System.out.println();

        // inserting a new element into an arbitrary list's position
        int pos = 0;
        System.out.println("inserting a new element into the list at position " + pos);
        DNAContainerUtils.insertIntoList(seqContainer, listId, pos, coder.encode("hello new element!"));
        printList(seqContainer, listId, coder);
    }

    static void printList(DNAContainer dnaContainer, long listId, Coder<String, BaseSequence> coder) {
        var listIterator = DNAContainerUtils.getListIterator(dnaContainer, listId);
        System.out.println("printing the list's elements");
        while(listIterator.hasNext())
            System.out.println("\t" + coder.decode(listIterator.next()));
    }
}
