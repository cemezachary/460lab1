package colgatedb.tuple;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.ArrayList;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A helper class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private static final long serialVersionUID = 1L;
    private TDItem[] schema;

    /**
     * Create a new TupleDesc with typeAr.length fields of the
     * specified types and with associated names.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr){
        if (typeAr.length == 0 || typeAr.length != fieldAr.length){
            schema = new TDItem[0];
            return;
        }
        schema = new TDItem[typeAr.length];
        for (int i = 0; i < schema.length; i++){
            schema[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types.  Field names should be assigned as empty
     * strings.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        if (typeAr.length == 0){
            schema = new TDItem[0];
            return;
        }
        schema = new TDItem[typeAr.length];
        for (int i = 0; i < schema.length; i++){
            schema[i] = new TDItem(typeAr[i], "");
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields(){
        return schema.length;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) {
        if (i >= schema.length || i < 0){
            throw new NoSuchElementException();
        }
        return schema[i].fieldType;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) {
        if (i >= schema.length || i < 0){
            throw new NoSuchElementException();
        }
        return schema[i].fieldName;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name)  {
        for (int i = 0; i < schema.length; i++) {
            if (schema[i].fieldName != null){
                if ((schema[i].fieldName).equals(name)) {
                    return i;
                }
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.  The size
     * depends on the field types.
     *
     * @see Type#getLen()
     */
    public int getSize() {
        int total = 0;
        for (int i = 0; i < schema.length; i++){
            total += (schema[i].fieldType.getLen());
        }
        return total;
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // hint: use java.util.Arrays.asList to convert array into a list, then return list iterator.
        Iterator<TDItem> iter = (Arrays.asList(schema).iterator());
        return iter;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o instanceof TupleDesc == false){
            return false;
        }
        TupleDesc cmp = (TupleDesc) o;
        if (cmp.getSize() == schema.length){
            for (int i = 0; i < schema.length; i++){
                if (cmp.getFieldType(i) != schema[i].fieldType){
                    return false;
                }
            }
        }
        else{
            return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results.  If you're not
        // sure yet (you may not be), then leave the UnsupportedOperationException
        // so that if this method ever gets called, it will trigger an exception.
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])".
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String desc = "";
        for (int i = 0; i < schema.length; i++){
            if (i != schema.length - 1){
                desc += schema[i].fieldName + "(" + schema[i].fieldType + ")" + ",";
            }
            desc += schema[i].fieldName + "(" + schema[i].fieldType + ")";
        }
        return desc;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int size = td1.numFields() + td2.numFields();
        String[] fields = new String[size];
        Type[] types = new Type[size];
        for (int i = 0; i < td1.getSize(); i++){
            fields[i] = (td1.getFieldName(i));
            types[i] = (td1.getFieldType(i));
        }
        for (int k = td1.getSize()-1; k < size; k++){
            fields[k] = (td1.getFieldName(k));
            types[k] = (td1.getFieldType(k));
        }
        TupleDesc desc = new TupleDesc(types, fields);
        return desc;
    }


}
