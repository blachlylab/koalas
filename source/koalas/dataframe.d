module koalas.dataframe;
import koalas.view;
import koalas.groupby;
import koalas.util;

import std.array: array;
import std.array: split, join;
import std.stdio;
import std.meta: AliasSeq, Stride;
import std.conv: to;
import std.traits: isSomeChar, isArray, isSomeString;
import std.typecons: tuple, Tuple;
import mir.ndslice;
import mir.algorithm.iteration;
import mir.ndslice.sorting;

alias GetNames(Args...) = Stride!(2, Args[1..$]);
alias GetTypes(Args...) = Stride!(2, Args);

struct Dataframe(Args...){
    alias RT = Tuple!(Args);
    RT[] records;

    alias recordType = RT;
    alias memberTypes = GetTypes!Args;
    alias memberNames = GetNames!Args;

    /// Allow getting column as a property
    /// TODO: fix issue when a column name overlaps existing df function
    /// e.g. column name length and length function below
    /// for now we will prevent these names as column names from compiling
    static foreach(i,member;memberNames){
        static assert(member != "length");
        static assert(member != "shape");
        static assert(member != "copy");
        static assert(member != "columns");
        static assert(member != "sort");
        static assert(member != "head");
        static assert(member != "toString");
        static assert(member != "unique");
        static assert(member != "records");
        mixin("alias "~member~" = getCol!\""~member~"\";");
    }

    this(RT[] rows){
        this.records = rows;
    }

    /// get number of rows
    @property ulong length(){
        return records.length;  
    }

    /// set number of rows
    @property void length(ulong len){
        records.length = len;
    }

    /// get shape of dataframe
    @property auto shape(){
        return tuple(records.length,columns.length);
    }

    /// get a copy of the dataframe
    auto copy(){
        return Dataframe!Args(records.dup);
    }

    /// returns a dataframe of filtered records
    /// filters on col == value if provided cmpOp is ==
    /// similar to df[df["col"] == value] in pandas
    template select(string col, string cmpOp = "=="){
        auto select(E)(E val) if(__traits(hasMember, RT, col))
        {
            mixin("static assert(is(typeof(RT." ~ col ~") == E));"); 
            mixin("auto fun = (RT x) => x." ~ col ~" "~ cmpOp ~" val;");
            return Dataframe!T(records.filter!fun.array);
        }
    }

    /// Returns an array of T items that are of the same
    /// type as the col type in question
    /// similar to df["col"] in pandas
    auto getCol(string col)() if(__traits(hasMember, RT, col))
    {
        return records.sliced.member!(col);
    }

    /// Adds a new column to dataframe (will be empty column)
    /// realistically creates a new DF type since we are statically typed
    auto addNewCol(T,string name)(){

        // make new dataframe and copy
        Dataframe!(Args, T, name) newdf;
        newdf.records.length = this.records.length;
        static foreach (c; columns)
        {
            newdf.getCol!(c)()[] = this.getCol!(c)()[];
        }
        return newdf;
    }

    /// reads dataframe from a file
    /// again must predefine columns with struct
    /// similar to pd.read_table in pandas
    void fromTable(string fn, string sep = "\t",int indexCols = 0, int headerCols = 0){
        auto file =  File(fn);
        string line;
        while(headerCols){
            file.readln();
            headerCols--;
        }
        while ((line = file.readln()) !is null){
            RT item;
            line = line[$-1] == '\n' ? line[0..$-1] : line; 
            auto fields = line.split(sep);
            fields = fields[indexCols .. $];
            static foreach (i,member; memberTypes)
            {
                mixin("item."~memberNames[i]~"=fields["~i.to!string~"].to!member;");
            }
            this.records ~= item;
        }
    }

    void toCsv(string fn, string sep = ",",bool writeHeader = true,bool writeIndex = false){
        auto file =  File(fn,"w");
        toCsv(file,sep,writeHeader,writeIndex);
    }

    /// writes a dataframe to a csv file
    /// similar to pd.read_table in pandas
    void toCsv(File file, string sep = ",",bool writeHeader = true,bool writeIndex = false){
        string line;
        if(writeHeader) file.writeln(columns.join(sep));
        foreach (i,rec; records)
        {
            if(writeIndex) line~= i.to!string ~ sep;
            static foreach (c; columns)
            {
                mixin("line ~= rec."~c~".to!string ~ sep;");
            }
            line = line[0..$-sep.length];
            file.writeln(line);
            line.length = 0;
        }
    }

    /// returns string[] of columns 
    /// similar to df.columns in pandas
    static string[] columns(){
        string[] ret;
        static foreach(m;memberNames){
            ret ~= m;
        }
        return ret;
    }

    /// Returns a groupby object grouped by a string[] of indices 
    /// similar to df.groupby(["col","col2"]) in pandas
    auto groupby(indices...)(){

        // generate index type
        alias Idx = Tuple!(Subset!([indices], Args));

        auto idx = records.makeIndex!(size_t, mirMultiSort!(Idx, RT));
        
        // make groupby
        Groupby!(Idx, Args) gby = Groupby!(Idx, Args)(&this, [], []);
        if(records.length == 0) return gby;
        gby.indexes ~= subsetTuple!(RT, [indices], Args)(records[idx[0]]);
        gby.groups ~= GroupbyItem!Idx(&gby.indexes[$-1], []);
        foreach (i; idx)
        {
            if(gby.indexes[$-1] == subsetTuple!(RT, [indices], Args)(records[idx[i]])) {
                gby.groups[$-1].items ~= i;
            } else {
                gby.indexes ~= subsetTuple!(RT, [indices], Args)(records[idx[i]]);
                gby.groups ~= GroupbyItem!Idx(&gby.indexes[$-1], [i]);
            }    
        }
        return gby;
    }

    /// sorts a dataframe based on a string[] of indices 
    View!(Args) sort(indices...)(){

        // generate indexes
        static foreach (index; indices)
        {
            static assert(__traits(hasMember, RT, index));
        }
        alias OT = Tuple!(Subset!([indices], Args));
        auto idx = records.makeIndex!(size_t, mirMultiSort!(OT, RT));
        return View!(Args)(&this, idx);
    }

    /// sorts a dataframe based on all columns 
    View!(Args) sort() {
        auto idx = records.makeIndex!(size_t, mirMultiSort!(RT, RT));
        return View!(Args)(&this, idx);
    }

    /// returns a dataframe of first n records 
    /// similar to df.head() in pandas
    auto head(ulong numRows = 5){
        auto idx = iota!(size_t, 1)(numRows).ndarray;
        return View!(Args)(&this, idx);
    }

    /// Converts dataframe to string for printing 
    string toString(){
        auto i = 0;
        string output = "\t"~columns.join("\t")~"\n";
        foreach (rec; records)
        {
            output~=i.to!string~"\t";
            static foreach (c; columns)
            {
                mixin("output~=rec."~c~".to!string;");
                output~="\t";
            }
            output = output[0..$-1] ~'\n';
            i++;
        }
        return output;
    }

    /// Returns a Dataframe of specific columns from current dataframe
    auto subset(string[] indices)(){
        alias RS = Tuple!(Subset!(indices, Args));
        alias fun = subsetTuple!(RT, indices, Args);
        return ApplyView!(fun, Args)(&this);
    }

    /// sort and get unique records from dataframe
    auto unique(){
        // auto tmp = this.copy;
        // this.records.sliced.sort;
        return Dataframe!Args(this.records.sliced.sort.uniq.array);
    }

    /// apply function to column elements
    /// returns an array of results
    auto apply(alias fun, string col)(){
        import std.functional : unaryFun, binaryFun;
        return map!fun(this.getCol!col);
    }

    /// apply function to column elements
    /// returns an array of results
    auto apply(string fun, string col)(){
        import std.functional : unaryFun;
        return map!(unaryFun!fun)(this.getCol!col);
    }

    /// apply function to all rows
    /// returns an array of results
    auto apply(alias fun)(){
        import std.functional : unaryFun, binaryFun;
        return map!fun(this.records);
    }

    /// apply function to all rows
    /// returns an array of results
    auto apply(string fun)(){
        import std.functional : unaryFun;
        return map!(unaryFun!fun)(this.records);
    }

    /// allow foreach on dataframe
    int opApply(int delegate(ref RT) dg) {
        int result = 0;

        foreach (rec; records) {
            result = dg(rec);

            if (result) {
                break;
            }
        }

        return result;
    }

    auto opIndex(R)(R indexes)
    {
        auto idx = iota!(size_t, 1)(indexes.length).filter!(x => indexes[x]).array;
        return View!(Args)(&this, idx);
    }

    void opOpAssign(string op: "~")(RT value)
    {
        records ~= value;
    }

    auto opAssign(Dataframe!(Args) rhs)
    {
        this.records = rhs.records;
        return this;
    }

    auto opAssign(View!(Args) rhs)
    {
        return this = rhs.fuse;
    }
}

auto unique(D)(D df){
    return D(df.sort.records.uniq.array);
}

auto concat(DF)(DF[] dfs ...){
    DF master;
    foreach (df; dfs)
    {
        master.records ~= df.records;
    }
    return master;
}

private struct testRecord{
    string chrom;
    int pos;
    string other;
}

/// returns string[] of columns 
/// similar to df.columns in pandas
static string[] columns(RT)(RT row){
    alias memberNames = AliasSeq!(RT.tupleof);
    string[] ret;
    static foreach(m;memberNames){
        ret ~= m.stringof;
    }
    return ret;
}

unittest{
    import std.stdio;
    Dataframe!(string, "chrom", int, "pos", string, "other") df;
    df.fromTable("source/tests/data/test.tsv", "\t", 0, 1);
    assert(df.length == 5);
    assert(df.shape == tuple(5, 3));
    assert(df.columns == ["chrom", "pos", "other"]);
    assert(df.pos == [2, 2, 3, 7, 6]);
    df.pos[] = [2, 2, 3, 7, 7];
    assert(df.pos == [2, 2, 3, 7, 7]);
    df.pos[] = [2, 2, 3, 7, 6];
    assert(df.pos == [2, 2, 3, 7, 6]);
    auto gby = df.groupby!("chrom", "pos");
    assert(gby.count.count == [2, 1, 1, 1]);
    writeln(gby.first.other);
    assert(gby.first.other == ["hi", "high", "no", "no"]);
    df.other[] = "j";
    df = df.sort!("chrom", "pos");
    df = concat(df,df);
    assert(df.apply!("a.to!string","pos") == ["2", "2", "3", "6", "7", "2", "2", "3", "6", "7"]);
    assert(df.apply!("a.pos * 2") == [4, 4, 6, 12, 14, 4, 4, 6, 12, 14]);
    auto sub = df.subset!(["chrom","pos"]).fuse();
    assert(sub.unique.chrom == ["1", "2", "q", "q"]);
    df.toCsv("/tmp/koalas.test.csv");
    // assert(sub.chrom.unique == ["1","2","q"]);
    foreach(ref rec;sub){
        writeln(rec);
    }
    auto index = sub.apply!("a > 5","pos");
    assert(sub[index].pos == [6,6,7,7]);
    df = df.sort();
    auto index2 = df.apply!("a > 5","pos");
    assert(df[index2].pos == [6,6,7,7]);
    assert(df.head.fuse.length == 5);
    // assert(sub[index].pos == [6,6,7,7]);
    // assert(sub.records[0].columns == sub.columns);

    auto df2 = df.addNewCol!(int, "test");
    assert(df2.columns == ["chrom", "pos", "other", "test"]);
}