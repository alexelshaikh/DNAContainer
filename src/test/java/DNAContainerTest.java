import core.BaseSequence;
import datastructures.container.AddressTranslationManager;
import datastructures.container.Container;
import datastructures.container.DNAContainer;
import datastructures.container.translation.DNAAddrTranslationManager;
import datastructures.container.utils.DNAContainerUtils;
import packaging.Application;
import utils.Coder;
import utils.lsh.minhash.BitMinHashLSH;
import utils.rq.RQCoder;
import java.util.Iterator;
import java.util.Objects;

public class DNAContainerTest {
    public static void main(String[] args) {
        int r = 5;
        int k = 6;
        long nBits = 20L;

        int addressSize = 80;
        int payloadSize = 170;
        int addressPermutations = 8;
        int payloadPaddingSize = 10;
        int payloadPermutations = 8;


        AddressTranslationManager<Long, BaseSequence> atm = DNAAddrTranslationManager
                .builder()
                .setLsh(BitMinHashLSH.newLSHForBaseSequences(k, r, nBits))
                .setAddrSize(addressSize)
                .setNumPermutations(addressPermutations)
                .setContainers(Container.discardingContainer(), Container.discardingContainer(), false, false)
                .build();


        DNAContainer seqContainer = DNAContainer
                .builder()
                .setAddressTranslationManager(atm)
                .setOligoLSH(BitMinHashLSH.newLSHForBaseSequences(k, r, nBits))
                .setPayloadSize(payloadSize)
                .setNumGcCorrectionsPayload(payloadPaddingSize)
                .setNumPayloadPermutations(payloadPermutations)
                .build();

        //var coder = RotatingTre.INSTANCE;
        //var coder = RotatingQuattro.INSTANCE;
        //var coder = Bin.INSTANCE;
        var coder = Coder.fuse(Application.BIJECTIVE_STRING_CODER, RQCoder.DEFAULT_RQ); // fountain code

        Container<Long, String, BaseSequence> container = Container.transform(seqContainer, coder);

        // reference
        long referenceId = container.put("hello world");
        System.out.println("getting the reference with id: " + referenceId + " -> " + container.get(referenceId));

        // array
        long arrayId = DNAContainerUtils.putArray(seqContainer, coder.encode("hello world 1"), coder.encode("hello world 2"), coder.encode("hello world 3"));
        System.out.println("getting the elements of the array of id: " + arrayId);

        // number of objects in the array
        int arrayLength = DNAContainerUtils.getArrayLength(seqContainer, arrayId);
        for (int i = 0; i < arrayLength; i++)
            System.out.println("\tpos: " + i + " -> " + coder.decode(Objects.requireNonNull(DNAContainerUtils.getArrayPos(seqContainer, arrayId, i))));

        // list
        long listId = DNAContainerUtils.putList(seqContainer, coder.encode("hello world 1"), coder.encode("hello world 2"), coder.encode("hello world 3"));
        Iterator<BaseSequence> listIterator = DNAContainerUtils.getListIterator(seqContainer, listId);
        System.out.println("printing the list's elements");
        while(listIterator.hasNext())
            System.out.println("\t" + coder.decode(listIterator.next()));

        // appending to the list
        System.out.println("successfully appended a new element to the list: " + DNAContainerUtils.appendToList(seqContainer, listId, coder.encode("hello world 4")));

        System.out.println("printing the list again");
        listIterator = DNAContainerUtils.getListIterator(seqContainer, listId);
        while(listIterator.hasNext())
            System.out.println("\t" + coder.decode(listIterator.next()));
    }
}
