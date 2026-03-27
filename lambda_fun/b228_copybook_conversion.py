# This is the python code to convert the Cobol copybook to the configuration file.

copy_book_file = "C:\\MThota\\DPM\\WFS\\ICSBRCCPPI-2115\\files\\b228_copybook.txt"
#copy_book_file = "C:\\MThota\\DPM\\WFS\\ICSBRCCPPI-2115\\files\\PledgeCopybook.txt"
configuration_file = "C:\\MThota\\DPM\\WFS\\ICSBRCCPPI-2115\\files\\b228_config.txt"
input_config_file = "C:\\MThota\\DPM\\WFS\\ICSBRCCPPI-2115\\files\\InputLayout.cfg"

# Check if copy_book_file ends with 'b228_copybook.txt'
if copy_book_file.endswith('b228_copybook.txt'):
    field_name_prefix_1 = ":B228:-"
    field_name_prefix_2 = ":B228:"
    field_name_prefix_3 = "B228:"
else:
    field_name_prefix_1 = "B228-"
    field_name_prefix_2 = "B228"
    field_name_prefix_3 = "B228"

file_config = open(configuration_file, "w")

tmp_base_array = []
skip_rec_type = []
base_array = []
occurs_array = []
pre_base_array = []
redefines_dict = {}
redefines_catch_dict = {}
redefines_occurs_dict = {}
redefines_occurs_catch_dict = {}
redefines_inner_occurs_dict = {}
redefines_inner_occurs_catch_dict = {}
replacement_dict = {}
occurs_cntr = 0
filter_dict = {}
stored_table_name = ""
ignoreValueReplace = False


def hdr():
    """
    This function is to create the header lines of the output configuration file.  It is only called once at the start of this program's execution.
    :return:
    """
    hdr1 = "# Field Type Description:\n"
    hdr2 = "#          X  = String\n"
    hdr3 = "#          N  = Numeric\n"
    hdr4 = "#       COMP  = Computational\n"
    hdr5 = "#      COMP3  = Computational 3\n"
    hdr6 = "#\n"
    hdr7 = "#-----|----------|----------------------------------------|------|------|------|----------|-----------|-----------------------------------------------------|------------------------------|\n"
    hdr8 = "# Sl  | COPYBOOK |              FIELD NAME                | START| END  |LENGTH| IN FORMAT| OUT FORMAT| OCCURS                                              | REDEFINES                    |\n"
    hdr9 = "#     |          |                                        |      |      |      |          |           |                                                     |                              |\n"
    hdr = hdr1 + hdr2 + hdr3 + hdr4 + hdr5 + hdr6 + hdr7 + hdr8 + hdr9
    file_config.write(hdr)


def hdr_base(name_prefix):
    """
    This function is used to add the sub header row that contains the name field_name_prefix_1.  Example ... XBASE RECORD.  It is called each time a new record type is started.
    :param nameprefix:
    :return:
    """
    name_prefix = name_prefix + " RECORD"
    hdr_name = name_prefix.center(40, '-')

    hdr_row = "#-----|----------|" + hdr_name + "|------|------|------|----------|-----------|-----------------------------------------------------|------------------------------|\n"
    file_config.write(hdr_row)


def entry_line(knt, copy_booknme, field_name, startpos, endpos, reclen, infmt, outfmt, has_occurs, occurscnt,
               redefines_tag, has_inner_occurs, occurscnt_inner, tableName):
    """
    This function is used to write the row to the config output file. It also contains the occurs data.  The module has for loops that repeat based on the
    occurs counts(occurscnt & occurscnt_inner).
    :param name:
    :param knt:
    :param copy_booknme:
    :param field_name:
    :param startpos:
    :param endpos:
    :param reclen:
    :param infmt:
    :param outfmt:
    :param has_occurs:
    :param occurscnt:
    :param redefines_tag:
    :param has_inner_occurs:
    :param occurscnt_inner:
    :return:
    """
    knt = str(knt).rjust(6)
    copy_booknme = copy_booknme.center(10)
    field_name = field_name.ljust(40)
    startpos = str(startpos).rjust(5)
    endpos = str(endpos).rjust(5)
    reclen = str(reclen).rjust(5)
    infmt = infmt.ljust(9)
    outfmt = outfmt.ljust(10)
    s1Cntr = knt
    start_field = startpos
    end_field = endpos
    rdftag = redefines_tag.ljust(28)
    inner_occurs_found = False
    tableNameSuffix = tableName.replace(".", "")

    global stored_table_name

    intlcntr = 0

    # If there is is an occurs block of data it will be followed by a row that has "END OCCURS" that is the signal that
    # the occurs block has completed.  Once the occurs block has completed then the occurs_array will be read in a for loop and
    # based on the occurs count.
    global ignoreValueReplace
    if "END OCCURS" in copy_booknme.strip():
        firstNameValue = 0
        lastName = 0
        for i in range(int(occurscnt)):
            occurscnter = i + 1

            fillerFound = False
            for line in occurs_array:

                # I need to get the S1 counts and the start and end positions in the Occurs block
                intlarray = line.upper().split("|")
                s1 = intlarray[0]
                cpybk = intlarray[1]
                fldname = intlarray[2]
                strtfld = intlarray[3]
                endfld = intlarray[4]
                length = intlarray[5]
                infrmt = intlarray[6]
                outfrmt = intlarray[7]

                if ignoreValueReplace and "1ST-NAME" in line:
                    firstNameValue = int(s1Cntr) + 1

                if ignoreValueReplace and "LAST-NAME" in line:
                    lastName = int(s1Cntr) + 2
                if not "OCCURS" in line or ("OCCURS" in line and "BMS" in line and not line.split("|")[1].strip()=="OCCURS" and  not line.split("|")[1].strip()=="END OCCURS"):
                    intlcntr = intlcntr + 1
                    if intlcntr == 1:
                        s1Cntr = s1.strip()
                        start_field = strtfld.strip()
                        end_field = endfld.strip()
                        lenfield = length.strip()
                        prevend_field = end_field
                    else:

                        s1Cntr = int(s1Cntr) + 1
                        start_field = int(end_field) + 1

                        lenfield = length.strip()
                        end_field = int(start_field) + (int(lenfield) - 1)
                        prevend_field = end_field
                        s1Cntr = str(s1Cntr).rjust(6)
                        start_field = str(start_field).rjust(5)
                        end_field = str(end_field).rjust(5)

                        if ignoreValueReplace and "1ST-NAME" in line:
                            startPosFirstname = int(start_field)
                        if fillerFound:
                            s1Cntr = str(lastName).rjust(6)
                            fillerFound = False

                        if ignoreValueReplace and "FILLER1" in line:
                            fillerFound = True

                        if "COMP-NAME" in line:
                            s1Cntr = str(firstNameValue).rjust(6)
                            start_field = str(startPosFirstname).rjust(5)
                            end_field = int(startPosFirstname) + (int(lenfield) - 1)
                            end_field = str(end_field).rjust(5)

                        # The following block of code is to accomodate an occurs block inside of another occurs block
                        if has_inner_occurs:
                            # The following clode block is to check for any redefines in the occurs block that is
                            # embeded inside of another occurs block.  It is done this way because the inner occurs
                            # data will not contain the same data added in the original redefines dictionaries
                            inner_search_key = fldname[6:].strip()
                            inner_catch_value = redefines_inner_occurs_dict.get(inner_search_key.strip())
                            if not inner_catch_value is None:
                                inner_redefines_occurs_value = s1Cntr.strip() + "|" + start_field.strip()
                                redefines_inner_occurs_catch_dict.update(
                                    {inner_catch_value.strip(): inner_redefines_occurs_value})

                            # The code block below checks the dictionary created above to get the s1, and start position
                            # for any redefines for an inner occurs.
                            inner_redefines_catch_value = redefines_inner_occurs_catch_dict.get(
                                inner_search_key.strip())
                            if not inner_redefines_catch_value is None:
                                inner_redefines_array = inner_redefines_catch_value.split("|")
                                s1Cntr = str(inner_redefines_array[0]).rjust(6)
                                start_field = str(inner_redefines_array[1]).rjust(5)
                                tmpend_field = int(start_field) + ((int(lenfield)) - 1)
                                end_field = str(tmpend_field).rjust(5)

                                inner_occurs_found = True

                        # The code block below check the dictionary created below that creates the "redefines_occurs_catch_dict"
                        # dictionary to get the s1, and start position for any redefines for an regular/outer occurs.
                        catch_key = fldname[6:].strip()
                        catch_value = redefines_occurs_catch_dict.get(catch_key.strip())
                        if not catch_value is None:
                            catchArray = catch_value.split("|")
                            redefineS1 = catchArray[0]
                            redefinestartpos = catchArray[1]
                            redefineEndPos = int(redefinestartpos) + (int(lenfield) - 1)

                            s1Cntr = str(redefineS1).rjust(6)
                            start_field = str(redefinestartpos).rjust(5)
                            end_field = str(redefineEndPos).rjust(5)

                        if stored_table_name != "":
                            # This is the line string that will eventually be written to the output config file for
                            # any occurs clause.
                            line = s1Cntr + "|" + cpybk + "|" + fldname + "|" + start_field + " |" + end_field + " |" + length + "|" + infrmt + "|" + outfrmt + "|" + " OCCURS  " + str(
                                occurscnter).ljust(4) + " - " + str(occurscnt).ljust(
                                4) + "* " + stored_table_name.ljust(30) + " |" + rdftag + "  |" + "\n"
                            # This is the line string that will eventually be written to the output config file for any occurs clause.
                        else:
                            line = s1Cntr + "|" + cpybk + "|" + fldname + "|" + start_field + " |" + end_field + " |" + length + "|" + infrmt + "|" + outfrmt + "|" + " OCCURS  " + str(
                                occurscnter).ljust(4) + " - " + str(occurscnt).ljust(
                                4) + " |" + rdftag + "  |" + "\n"

                        # If there is an inner occurs block then every row after the first one will be created in the block of
                        # code below.  It will repeat based on the occurscnt_inner
                        if inner_occurs_found:
                            for x in range(int(occurscnt_inner) - 1):
                                s1Cntr = int(s1Cntr) + 1
                                news1CntrNew = s1Cntr
                                start_field = int(end_field) + 1
                                end_field = int(start_field) + (int(length) - 1)

                                # Formatting the new fields for the row
                                news1CntrNew = str(news1CntrNew).rjust(6)
                                start_field = str(start_field).rjust(5)
                                end_field = str(end_field).rjust(5)

                                newLine = news1CntrNew + "|" + cpybk + "|" + fldname + "|" + str(
                                    start_field) + " |" + str(end_field) + " |" + str(
                                    length) + "|" + infrmt + "|" + outfrmt + "|" + " OCCURS  " + str(
                                    occurscnter).ljust(4) + " - " + str(occurscnt).ljust(
                                    ###    4) + "* " + stored_table_name.ljust(53) + " |" + rdftag + "  |" + "\n"
                                    4) + "* " + tableNameSuffix.ljust(30) + " |" + rdftag + "  |" + "\n"

                                line = line + newLine

                    rfKey = fldname[6:].strip()

                    # The code block below checks the dictionary(redefines_occurs_dict) created in the "base" module to
                    # get the redefines data. It created the  elow that creates the "redefines_occurs_catch_dict" dictionary
                    # that will be checked to get the s1, and start position for any redefines for an regular/outer occurs.
                    itemCntr = 0
                    tmprfKey = ""
                    for item in redefines_occurs_dict:
                        itemCntr = itemCntr + 1
                        tmprfKey = str(itemCntr) + "|" + rfKey
                        value = redefines_occurs_dict.get(tmprfKey.strip())
                        if not value is None:
                            tmp_redefineS1 = int(s1Cntr)
                            tmp_redefines_startpos = int(start_field)
                            redefinesStr = str(tmp_redefineS1) + "|" + str(tmp_redefines_startpos)
                            redefines_occurs_catch_dict[value.strip()] = redefinesStr

                    occursrow = line.replace("XXXXXX", "OCCURS")
                    if "06100" in line:
                        occursrow = occursrow.replace("OCCURS 2 TIMES", "     ")

                    # Writing the occurs row to the output config file
                    file_config.write(occursrow)

                    inner_occurs_found = False

        occurs_array.clear()
        knt = s1Cntr
        stored_table_name = ""

    # The following code will load any occurs block of data into the occurs_array.  The array will be unloaded and written
    # to the config file after the occurs block of data has been received.  It will be signaled by receiving the "END_OCCURS"
    # if neither of the has_occurs booleans are true then the data will be written to the output config file at this time.
    if has_occurs or has_inner_occurs:
        if len(occurs_array) == 0 or "22010" in tableNameSuffix:
            if "PIC" in tableNameSuffix:
                array_tableNameSuffix = tableNameSuffix.split("PIC")
                stored_table_name = array_tableNameSuffix[0].strip()
            else:
                stored_table_name = tableNameSuffix

        row = str(knt) + "|" + copy_booknme + "|" + field_name + "|" + str(startpos) + " |" + str(endpos) + " |" + str(
            reclen) + " | " + infmt + "| " + outfmt + "|" + " XXXXXX  " + "1   " + " - " + str(occurscnt).ljust(
            4) + "* " + stored_table_name.ljust(30) + " |" + rdftag + "  |" + "\n"
        ##            4) + "* " + tableNameSuffix.ljust(30) + " |" + rdftag + "  |" + "\n"
        occurs_array.append(row)
    else:
        arraylen = 0

        row = str(knt) + "|" + copy_booknme + "|" + field_name + "|" + str(startpos) + " |" + str(endpos) + " |" + str(
            reclen) + " | " + infmt + "| " + outfmt + "|" + "                                                     | " + rdftag + " |" + "\n"
        if not "OCCURS" in row:
            file_config.write(row)

    # The s1, start_position and end_position have to be returned so that the correct positions can be returned t the calling function
    rtnsrting = str(knt) + "|" + str(start_field).strip() + "|" + str(end_field).strip()

    return rtnsrting


def temp_base(rectype):
    """
    This function is to standardize the line that wil be fed to the rest of the program. It combines all lines in the copybook
    until it gets to a period.
    :return:
    """
    cnt = 0
    item_kntr = 0
    tmpline = ""
    skip_filter = False

    if rectype in filter_dict:
        filters = filter_dict[rectype]
    else:
        filters = []

    for line in tmp_base_array:
        # Added the line below
        item_kntr = 0
        if line.endswith('.'):
            tmpline = tmpline + line

            value = replacement_dict.get(tmpline.strip())
            for filter_entry in filters:
                fromIndexVal = filter_entry['from']
                if fromIndexVal != "" and tmpline.strip().startswith(fromIndexVal):
                    if not skip_filter:
                        skip_filter = True

            if not skip_filter:
                if value is not None:
                    value_array = value.split(",")
                    for knt in value_array:
                        pre_base_array.append(value_array[item_kntr].strip())
                        item_kntr += 1
                    tmpline = ""
                else:
                    pre_base_array.append(tmpline.strip())
                    tmpline = ""
            else:
                for filter_entry in filters:
                    toIndexVal = filter_entry['to']
                    if toIndexVal != "" and tmpline.strip() == toIndexVal:
                        skip_filter = False
                tmpline = ""
        else:
            tmpline = tmpline + "  " + line + "  "

    cnt += 1


def redefines_fix():
    """
    This function parses the B228 copybook and looks for any REDEFINES so that it can build the inital redefines dictionary
    The "redefines_cntr" is added to the dictionary key as some of the redefine values point to the same element.
    :return:
    """
    prevlvlnum = 0
    lvlnum = 0
    redefines = False
    redefines_cntr = 0

    for line in pre_base_array:
        if "REDEFINES" in line:
            redefineArray = line.split("REDEFINES")

            if len(redefineArray) > 1:
                rfItem1 = redefineArray[0]
                rfItem2 = redefineArray[1]
                rfDict1 = ""

                rfArray1 = rfItem1.split(field_name_prefix_1)
                if len(rfArray1) > 1:
                    rfDict1 = rfArray1[1]

                rfArray2 = rfItem2.split("PIC")
                if len(rfArray2) > 1:
                    rfDict2 = rfArray2[0].strip()
                    rfDict2 = rfDict2.replace(field_name_prefix_1, "")
                    rfDict3Pic = rfArray2[1].strip()
                    rfDict3Pic = rfDict3Pic.replace(".", "")
                else:
                    rfDict2 = rfArray2[0].strip()
                    rfDict2 = rfDict2.replace(field_name_prefix_1, "")
                    rfDict2 = rfDict2.replace(".", "")
                    rfDict3Pic = ""

                redefines_cntr = redefines_cntr + 1
                newStr = rfItem1.strip()

                ### Need to accomodate for the OCCURS claus in the REDEFINES.
                ### The split below will remove the data following the OCCURS clause from the key if the dictionary
                if "OCCURS" in rfDict2.strip():
                    keyArray = rfDict2.strip().split("OCCURS")
                    newKey = str(redefines_cntr) + "|" + keyArray[0]
                else:
                    newKey = str(redefines_cntr) + "|" + rfDict2.strip()

                redefines_dict.update({newKey.strip(): newStr.strip()})

        if not redefines:
            redefines = True
            lvlnum = line[0:2]
            prevlvlnum = 99

        prevlvlnum = line[0:2]

        if int(prevlvlnum) <= int(lvlnum):
            redefines = False

        if not redefines:
            base_array.append(line.strip())
            prevlvlnum = 0


def find_table_name(input_array, name_prefix, currentIndexVal):
    occuresFound = ""
    ##    for line in input_array:
    ##        if "X3ANNU-DETAIL" in line.strip():
    ##            print("field_name " + currentIndexVal)
    for line in input_array:
        if "90100" in name_prefix:
            field_name = "90100-TRAILER-TABLE"
            return field_name

        if "OCCURS" in line or occuresFound != "":
            if occuresFound != "" and "OCCURS" in line:
                occuresFound = line.split("OCCURS")[0].strip()
            elif occuresFound == "":
                occuresFound = line.split("OCCURS")[0].strip()
            if (currentIndexVal.replace(name_prefix, "").strip() in line or currentIndexVal.replace(name_prefix + "-",
                                                                                                    "").strip() in line) and occuresFound != "":

                if "-DATA" in line.replace(currentIndexVal.replace(name_prefix, "").strip(), ""):
                    continue
                if "PIC" in line and "FILLER" in currentIndexVal:
                    tmp_name_array = occuresFound.upper().split(" ")
                    if len(tmp_name_array) > 2:
                        field_name = tmp_name_array[2]
                    elif len(tmp_name_array) > 1:
                        field_name = tmp_name_array[1]
                    else:
                        tmp_name_array = line.upper().split(" ")
                        field_name = name_prefix + "-" + tmp_name_array[0].strip()
                    return field_name

                if "PIC" in line and "OCCURS" in line:
                    previewVal = input_array[input_array.index(line) - 1]
                    tmp_name_array = previewVal.upper().split(field_name_prefix_3)
                    if len(tmp_name_array) > 2:
                        field_name = name_prefix + tmp_name_array[2]
                    elif len(tmp_name_array) > 1:
                        field_name = name_prefix + tmp_name_array[1]
                    else:
                        tmp_name_array = line.upper().split(" ")
                        field_name = name_prefix + "-" + tmp_name_array[0].strip()
                    return field_name
                if "OCCURS" in line:
                    previewVal = input_array[input_array.index(line) - 1]
                    if field_name_prefix_3 in previewVal:
                        tmp_name_array = previewVal.upper().split(field_name_prefix_3)
                        if len(tmp_name_array) > 2:
                            field_name = name_prefix + tmp_name_array[2]
                        elif len(tmp_name_array) > 1:
                            field_name = name_prefix + tmp_name_array[1]
                        else:
                            tmp_name_array = line.upper().split(" ")
                            field_name = name_prefix + "-" + tmp_name_array[0].strip()
                    return field_name

                else:
                    tmp_name_array = occuresFound.upper().split(field_name_prefix_3)
                    if len(tmp_name_array) > 2:
                        field_name = name_prefix + tmp_name_array[2]
                    elif len(tmp_name_array) > 1:
                        field_name = name_prefix + tmp_name_array[1]
                    else:
                        tmp_name_array = line.upper().split(" ")
                        field_name = name_prefix + "-" + tmp_name_array[0].strip()
                    return field_name
    #            else:
    #                print("field_name " + currentIndexVal)
    return " "


def base(namecpycook, nameprefix):
    """
    This function reads in the base_array copybook array(standardized array of the B228 copybook) and parses it to get the
    data needed for the Redefines and Occurs clauses.  It will also call the PIC clause function that will be used for formatting.
    This function will also call the "entry" function that writes the output config file.
    :param namecpycook:
    :param nameprefix:
    :return:
    """
    cnt = 0
    s1 = 0
    field_name = " "
    lvlnum = 0
    occurslvlnum = 0
    occurs_flag = False
    startpos = 0
    endpos = 0
    rtnfields = " "
    rtnfieldlen = 0
    rtninfmt = "N"
    rtnoutfmt = "X"
    tmppic = ""
    tmpfield_name = " "
    fillercnt = 0
    prevend = 0
    totlen = len(base_array)
    calc_totlen = totlen - 1
    hasoccurs = False
    occurscnt = 0
    prevoccurscnt = 0
    redefines_lvl_check = 0
    redefines = False
    skip_it = False
    rtrnstr = " "
    redefineS1 = " "
    redefine_end_pos = " "
    redefine_prev_end = " "
    catch_key = ""
    redefines_test_key = ""
    redefines_tag = ""
    redefines_lvl_first = " "
    redefines_lvl_second = " "
    first_part_redefines = False
    second_part_redefines = False
    needs_redefines_key = False
    found_value = ""
    found_key = ""
    needs_top_redefines_key = False
    inner_occurs = False
    occurs_cnt_inner = 0
    found_occurs_value = " "
    occurs_item_cntr = 0
    redefines_lvl_top = 0
    nameprefix = nameprefix
    global ignoreValueReplace

    for original_line in base_array:
        # if ignoreValueReplace:
        #     if ("FILLER" not in original_line):
        #         ignore_redifed_start_value.append(nameprefix + original_line.split("B228:")[1])
        # if ignoreValueReplace and "FILLER" in original_line and "PIC" in original_line:
        #     ignoreValueReplace = False
        line = original_line
        # The (needs_top_redefines_key) is used to determine the (s1, endpos, and prevend) values as they could not be
        # established on the Redefines line because there was not PIC clause.
        if needs_top_redefines_key and "PIC" in line:
            needs_top_redefines_key = False

            s1 = redefineS1
            endpos = redefine_end_pos
            prevend = redefine_prev_end

        redefines_lvl_check = line[0:2]
        # The (first_part_redefines & second_part_redefines) are used for determining the Redefines from and To names in
        # the last column of the output config file.
        if int(redefines_lvl_check) <= int(redefines_lvl_top):
            redefines_tag = ""

        if first_part_redefines and redefines_lvl_check <= redefines_lvl_first:
            first_part_redefines = False
        ##            redefines_tag = ""
        elif second_part_redefines and redefines_lvl_check <= redefines_lvl_second:
            second_part_redefines = False
        ##            redefines_tag = ""

        if "REDEFINES" in line:
            if "FILLER" in line:
                ignoreValueReplace = True
            redfarray1 = line.split("REDEFINES")
            redfitem1 = redfarray1[0]
            if "PIC" in line:
                redfarray2 = line.split("PIC")
                redfitem2 = " PIC" + redfarray2[1]
            else:
                redfitem2 = " "

            newPprevEnd = ""
            newRtnfieldlen = ""
            catch_key = redfitem1.strip()
            catchValue = redefines_catch_dict.get(catch_key.strip())
            if not catchValue is None:
                catchArray = catchValue.split("|")
                redefineS1 = catchArray[0]
                redefine_end_pos = catchArray[1]
                redefine_prev_end = catchArray[2]
                newStartPos = catchArray[3]
                newPprevEnd = catchArray[4]
                newRtnfieldlen = catchArray[5]
                keyValue = catchArray[6]

                redefines_lvl_first = line[0:2]

                if int(redefines_lvl_first) <= int(redefines_lvl_top) or int(redefines_lvl_top) == 0:
                    redefines_lvl_top = redefines_lvl_first

                    redefines_tag = "RDF_" + keyValue

                first_part_redefines = True

                if not "PIC" in line:
                    needs_top_redefines_key = True

            # The following block of code is to accomdate and Occurs block that comes inside of a Redefines block
            if "OCCURS" in line:
                if hasoccurs:
                    # If we are already in an occurs clause and we have another occurs clause then the inner_occurs flag
                    # gets set to true.
                    inner_occurs = True

                occurs_flag = True
                hasoccurs = True

                occursCheckArray1 = line.split("OCCURS")
                occursCntTmp = 0
                if "TIMES" in occursCheckArray1[1]:
                    occursCheckArray2 = occursCheckArray1[1].split("TIMES")
                    occursCntTmp = occursCheckArray2[0].strip()
                else:
                    occursCntTmp = occursCheckArray1[1].strip()

                if inner_occurs:
                    occurs_cnt_inner = occursCntTmp
                else:
                    occurscnt = occursCntTmp

                occurslvlnum = line[0:2]

            line = redfitem1 + redfitem2

            if "PIC" in line:
                s1 = redefineS1
                endpos = redefine_end_pos
                prevend = redefine_prev_end
            if "FILLER" in line:
                s1 = redefineS1
                endpos = redefine_end_pos
                prevend = redefine_prev_end

        if "PIC" in line and not "88  :" in line:
            picarray = line.upper().split("PIC")
            tmpfield_name = picarray[0]
            tmpnamearray = tmpfield_name.upper().split(field_name_prefix_2)

            if len(tmpnamearray) > 2:
                field_name = nameprefix + tmpnamearray[2]
            elif len(tmpnamearray) > 1:
                field_name = nameprefix + tmpnamearray[1]
            else:
                # To accomodate entries that do not have field_name_prefix_2 example... ETS90100
                tmpnamearray = tmpfield_name.upper().split("  ")
                field_name = nameprefix + "-" + tmpnamearray[1].strip()
        ### The else below is needed to get the field_name in case of a Redefines that does not have a PIC in the line
        else:
            tmpnamearray = line.upper().split(field_name_prefix_1)
            if len(tmpnamearray) > 1:
                field_name = tmpnamearray[1]
                field_name = field_name.replace(".", "")

        # The code below is removing the field_name_prefix_1 from the name field, so that it can be used to be a key in a later part of the code.
        # Example... 00000, XBASE
        nameprefixPlus = nameprefix + "-"
        redefines_test_key = field_name.replace(nameprefixPlus, "")

        itemCntr = 0
        tmprfKey = ""
        for item in redefines_dict:
            itemCntr = itemCntr + 1
            tmprfKey = str(itemCntr) + "|" + redefines_test_key
            value = redefines_dict.get(tmprfKey.strip())
            if not value is None:
                rfValueArray = value.split(field_name_prefix_1)
                if len(rfValueArray) > 1:
                    rfValue = rfValueArray[1]
                else:
                    rfValueArray = value.strip().split(" ")
                    rfValue = rfValueArray[1]

                redefines_lvl_second = line[0:2]

                if int(redefines_lvl_second) <= int(redefines_lvl_top) or int(redefines_lvl_top) == 0:
                    redefines_lvl_top = redefines_lvl_second

                    if len(rfValue.strip()) > 0:
                        redefines_tag = "RDF_" + rfValue

                second_part_redefines = True

        if not "REDEFINES" in line and not "PIC" in line and not "88  :" in line and not "OCCURS" in line:
            if field_name_prefix_2 in line:
                tmpnamearray = line.split(field_name_prefix_2)
                field_name = tmpnamearray[1]
                field_name = field_name.replace(".", "")

        if "88  :" in line:
            skip_it = True
        elif "OCCURS" in line:
            field_namearray = field_name.upper().split("OCCURS")
            field_name = field_namearray[0].strip()

            occurs_flag = True
            if "PIC" in line and "TIMES" in line:
                occurs_array = line.upper().split("OCCURS")
                occurs_array2 = occurs_array[1].split("TIMES")
                occurscnt = str(occurs_array2[0]).strip()
                occurslvlnum = line[0:2]
                hasoccurs = True
                pic = picarray[1]

                ## Calling the picture clause format (picdefine) code.  It will return the fieldlen, infmt and outfmt as part of a string
                ##that is pipe "|" delimited.
                rtnfields = str(picdefine(pic))
                ##The returned value is split into the rtnfieldlen, rtninfmt and rtnoutfmt
                rtnfieldsarray = rtnfields.split("|")
                rtnfieldlen = rtnfieldsarray[0]
                rtninfmt = rtnfieldsarray[1]
                rtnoutfmt = rtnfieldsarray[2]
                s1 = int(s1) + 1
                startpos = int(endpos) + 1
                endpos = int(prevend) + int(rtnfieldlen)
                prevend = endpos

                rtrnstr = entry_line(s1, namecpycook, field_name, startpos, endpos, rtnfieldlen, rtninfmt, rtnoutfmt,
                                     hasoccurs, occurscnt, redefines_tag, inner_occurs, occurs_cnt_inner,
                                     find_table_name(base_array, nameprefix, field_name))

            elif "PIC" in line:
                occurs_array = line.upper().split("OCCURS")
                occurs_array2 = occurs_array[1].split("PIC")
                occurscnt = str(occurs_array2[0]).strip()
                occurslvlnum = line[0:2]
                hasoccurs = True
                pic = picarray[1]

                ## Calling the picture clause format (picdefine) code.  It will return the fieldlen, infmt and outfmt as part of a string
                ##that is pipe "|" delimited.
                rtnfields = str(picdefine(pic))
                ##The returned value is split into the rtnfieldlen, rtninfmt and rtnoutfmt
                rtnfieldsarray = rtnfields.split("|")
                rtnfieldlen = rtnfieldsarray[0]
                rtninfmt = rtnfieldsarray[1]
                rtnoutfmt = rtnfieldsarray[2]
                s1 = int(s1) + 1
                startpos = int(endpos) + 1
                endpos = int(prevend) + int(rtnfieldlen)
                prevend = endpos

                rtrnstr = entry_line(s1, namecpycook, field_name, startpos, endpos, rtnfieldlen, rtninfmt, rtnoutfmt,
                                     hasoccurs, occurscnt, redefines_tag, inner_occurs, occurs_cnt_inner,
                                     find_table_name(base_array, nameprefix, field_name))

            elif "TIMES" in line:
                occurs_array = line.upper().split("OCCURS")
                occurs_array2 = occurs_array[1].split("TIMES")
                occurscnt = str(occurs_array2[0]).strip()
                occurslvlnum = line[0:2]
                hasoccurs = True
                ##To Accomodate the instance of an OCCURS clause happening right after another OCCURS clause
                if not prevoccurscnt == occurscnt:
                    rtrnstr = entry_line(s1, "END OCCURS", field_name, startpos, endpos, rtnfieldlen, rtninfmt,
                                         rtnoutfmt, hasoccurs, str(prevoccurscnt), redefines_tag, inner_occurs,
                                         occurs_cnt_inner, find_table_name(base_array, nameprefix, field_name))

                returnarray = rtrnstr.split("|")
                s1 = returnarray[0]
                startpos = returnarray[1]
                endpos = returnarray[2]
                prevend = endpos

                prevoccurscnt = occurscnt
                #####################

                rtrnstr = entry_line(s1, "OCCURS", field_name, startpos, endpos, rtnfieldlen, rtninfmt, rtnoutfmt,
                                     hasoccurs, str(occurscnt), redefines_tag, inner_occurs, occurs_cnt_inner,
                                     find_table_name(base_array, nameprefix, field_name))

        elif (line[0:2].isdigit() and not "PIC" in line):
            lvlnum = line[0:2]

            if not "OCCURS" in original_line and occurs_flag:
                if int(occurslvlnum) >= int(lvlnum):
                    hasoccurs = False

                    rtrnstr = entry_line(s1, "END OCCURS", field_name, startpos, endpos, rtnfieldlen, rtninfmt,
                                         rtnoutfmt, hasoccurs, str(occurscnt), redefines_tag, inner_occurs,
                                         occurs_cnt_inner, find_table_name(base_array, nameprefix, field_name))

                    catchValue = redefines_catch_dict.get(catch_key.strip())
                    if not catchValue is None:
                        catchArray = catchValue.split("|")
                        redefineS1 = catchArray[0]
                        redefine_end_pos = catchArray[1]
                        redefine_prev_end = catchArray[2]
                        s1 = redefineS1
                        endpos = redefine_end_pos
                        prevend = redefine_prev_end

                        s1 = int(s1) + 1
                        endpos = int(prevend) + 1
                        startpos = int(endpos) + 1

                        prevend = endpos

                    else:
                        returnarray = rtrnstr.split("|")
                        s1 = returnarray[0]
                        startpos = returnarray[1]
                        endpos = returnarray[2]
                        prevend = endpos

                    occurs_flag = False

        elif (line[0:2].isdigit() and "PIC" in line):
            lvlnum = line[0:2]

            # print('line  ', line)

            if not "OCCURS" in original_line and occurs_flag:
                if int(occurslvlnum) >= int(lvlnum):
                    hasoccurs = False

                    rtrnstr = entry_line(s1, "END OCCURS", field_name, startpos, endpos, rtnfieldlen, rtninfmt,
                                         rtnoutfmt, hasoccurs, str(occurscnt), redefines_tag, inner_occurs,
                                         occurs_cnt_inner, find_table_name(base_array, nameprefix, field_name))

                    returnarray = rtrnstr.split("|")
                    s1 = returnarray[0]
                    startpos = returnarray[1]
                    endpos = returnarray[2]

                    prevend = endpos

                    occurs_flag = False

            if cnt <= calc_totlen:
                if not cnt == calc_totlen:
                    nextrow = base_array[cnt + 1]

                else:
                    nextrow = base_array[cnt]

                nextlvl = nextrow[0:2]

                if nextlvl.isdigit():
                    s1 = int(s1) + 1
                    startpos = int(endpos) + 1

                    picarray = line.upper().split("PIC")
                    tmpfield_name = picarray[0]
                    if " FILLER " in tmpfield_name:
                        tmpnamearray = tmpfield_name.upper().split("FILLER")
                        pic = picarray[1]
                        fillercnt = fillercnt + 1
                        field_name = "FILLER" + str(fillercnt)

                        ## Calling the picture clause format (picdefine) code.  It will return the fieldlen, infmt and outfmt as part of a string
                        ##that is pipe "|" delimited.
                        rtnfields = str(picdefine(pic))
                        ##The returned value is split into the rtnfieldlen, rtninfmt and rtnoutfmt
                        rtnfieldsarray = rtnfields.split("|")
                        rtnfieldlen = rtnfieldsarray[0]
                        rtninfmt = rtnfieldsarray[1]
                        rtnoutfmt = rtnfieldsarray[2]

                    else:
                        pic = picarray[1]

                        ## Calling the picture clause format (picdefine) code.  It will return the fieldlen, infmt and outfmt as part of a string
                        ##that is pipe "|" delimited.
                        rtnfields = str(picdefine(pic))
                        ##The returned value is split into the rtnfieldlen, rtninfmt and rtnoutfmt
                        rtnfieldsarray = rtnfields.split("|")
                        rtnfieldlen = rtnfieldsarray[0]
                        rtninfmt = rtnfieldsarray[1]
                        rtnoutfmt = rtnfieldsarray[2]

                    endpos = int(prevend) + int(rtnfieldlen)
                    prevend = endpos

                    if not inner_occurs:
                        rtrnstr = entry_line(s1, namecpycook, field_name, startpos, endpos, rtnfieldlen, rtninfmt,
                                             rtnoutfmt, hasoccurs, occurscnt, redefines_tag, inner_occurs,
                                             occurs_cnt_inner, find_table_name(base_array, nameprefix, field_name))
                        if ignoreValueReplace:
                            print(rtrnstr)
                        # //here

        cnt = cnt + 1

        rfKey = ""
        if len(field_name.strip()) > 1:
            rfKeyTemp = field_name
            rfKeyArray = rfKeyTemp.split("-")

            rkCntr = 0
            for arrayKey in rfKeyArray:
                if rkCntr > 0:
                    if len(rfKey) > 1:
                        rfKey = rfKey + "-" + arrayKey
                    else:
                        rfKey = arrayKey

                rkCntr = rkCntr + 1

        ##Start Block -The following code is to read in a element from a Redefines if the block that was being redefined did not have a PIC
        if needs_redefines_key and "PIC" in line:
            needs_redefines_key = False
            # if()

            tmpredefineS1 = s1 - 1
            tmpredefine_end_pos = startpos - 1
            tmpredefine_prev_end = int(prevend) - int(rtnfieldlen)

            redefinesStr = str(tmpredefineS1) + "|" + str(tmpredefine_end_pos) + "|" + str(
                tmpredefine_prev_end) + "|" + str(startpos) + "|" + str(prevend) + "|" + str(
                rtnfieldlen) + "|" + found_key

            ##The code below is for Regular REDEFINES
            found_valueArray = found_value.split("|")
            for useValue in found_valueArray:
                if len(useValue.strip()) > 1:
                    redefines_catch_dict.update({useValue.strip(): redefinesStr})
            found_value = ""

            ##The code below is for OCCURS REDEFINES
            found_occurs_valueArray = found_occurs_value.split("|")
            for useOCCURSValue in found_occurs_valueArray:
                if len(useValue.strip()) > 1:
                    redefines_occurs_dict.update({useOCCURSValue.strip(): redefinesStr})
                    if "FILLER" in line:
                        innerKey = "FILLER"
                    else:
                        inner_occursLineArray = line.split(field_name_prefix_1)
                        inner_occursLine = inner_occursLineArray[1]

                        if not "PIC" in inner_occursLine:
                            innerKey = inner_occursLineArray[1]
                        else:
                            inner_occursLineArray2 = inner_occursLine.split("PIC")
                            innerKey = inner_occursLineArray2[0]

                    redefines_inner_occurs_dict.update({innerKey.strip(): useOCCURSValue})

            found_occurs_value = ""

        ##End Block

        # The following block of code checks the redefines key for (redefines_dict) dictionary that is built in the
        # Redefines function(redefinesfix()).  Because there can be multiple elements redefining the same element the key
        # is build with a sequential counter field_name_prefix_1.  Example 1|XNA6-INV-OBJ-CDATE-X.  The for loop add the sequential number
        # to the tempkey to check the dictionary.  If the key is found in the dictionary then a new dictionay(redefines_occurs_dict.update)
        # is updated with the value as the key and the new value with the (s1, startpos, prevend, etc...).
        # Example key -> 13  :B228:-XNA6-INV-OBJ-CDATE, value -> 11|41|41|42|46|5|XNA6-INV-OBJ-CDATE-X
        #
        # The Occurs dictionary(redefines_occurs_dict) also in case the Redefines needs to be used in the Occurs part of the
        # entry() function.  The key is different as only certain information will be available when the key is checked.
        # Example newrfKey -> 1|XNA6-INV-OBJ-CDATE-X, occursValue -> XNA6-INV-OBJ-CDATE
        #
        # If the PIC is not in the line then the next line with a PIC will be used.

        itemCntr = 0
        tmprfKey = ""
        for item in redefines_dict:
            itemCntr = itemCntr + 1
            tmprfKey = str(itemCntr) + "|" + rfKey
            value = redefines_dict.get(tmprfKey.strip())
            if not value is None:

                found_key = rfKey

                ##Start Block -The following code is to allow for a Redefines if the block that is being redefined does not have a PIC
                ##because it will not go through the basic code, so you want the next element that has a PIC.
                if "PIC" in line:
                    tmpredefineS1 = s1 - 1
                    tmpredefine_end_pos = startpos - 1
                    tmpredefine_prev_end = int(prevend) - int(rtnfieldlen)

                    redefinesStr = str(tmpredefineS1) + "|" + str(tmpredefine_end_pos) + "|" + str(
                        tmpredefine_prev_end) + "|" + str(startpos) + "|" + str(prevend) + "|" + str(
                        rtnfieldlen) + "|" + rfKey

                    redefines_catch_dict.update({value.strip(): redefinesStr})
                    occurs_item_cntr = occurs_item_cntr + 1

                    occursValurArray = value.split(field_name_prefix_1)
                    if field_name_prefix_1 in value:
                        occursValue = occursValurArray[1].strip()
                    else:
                        occursValue = occursValurArray[0].strip()

                    newrfKey = str(occurs_item_cntr).strip() + "|" + rfKey

                    redefines_occurs_dict.update({newrfKey.strip(): occursValue.strip()})
                else:
                    ## Code added because there can be more than one Redefines and they all need to be stored
                    found_value = found_value + "|" + value.strip()

                    occurs_item_cntr = occurs_item_cntr + 1

                    occursValurArray = value.split(field_name_prefix_1)
                    if field_name_prefix_1 in value:
                        occursValue = occursValurArray[1].strip()
                    else:
                        occursValue = occursValurArray[0].strip()

                    found_occurs_value = found_occurs_value + "|" + occursValue.strip()
                    newrfKey = str(occurs_item_cntr).strip() + "|" + rfKey.strip()

                    needs_redefines_key = True
                ##End Block
        ###End For Loop

    # If on the last row of the array and need to end the Occurs clause
    if occurs_flag:
        hasoccurs = False

        if inner_occurs:
            rtrnstr = entry_line(s1, namecpycook, field_name, startpos, endpos, rtnfieldlen, rtninfmt, rtnoutfmt,
                                 hasoccurs, occurs_cnt_inner, redefines_tag, inner_occurs, occurs_cnt_inner,
                                 find_table_name(base_array, nameprefix, field_name))

        rtrnstr = entry_line(0, "END OCCURS", " ", 0, 0, 0, " ", " ", hasoccurs, occurscnt, redefines_tag, inner_occurs,
                             occurs_cnt_inner, find_table_name(base_array, nameprefix, field_name))

        occurs_flag = False


def picdefine(pic):
    """
    The function below is to format the data from the picture clause.  There are several variations of the picture clause
    that are accomodated.
    :param pic:
    :return:
    """
    if "X(" in pic:
        tmppic = pic.replace("(", "")
        tmppic = tmppic.replace("X", "")
        tmppic = tmppic.replace(")", "")
        tmppic = tmppic.replace(".", "")
        tmpnum = tmppic.strip()
        fieldlen = int(tmpnum)
        infmt = "X"
        outfmt = "X"
    elif "X" in pic:
        tmppic = pic.replace(".", "")
        tmpnum = tmppic.strip()
        fieldlen = len(tmpnum)
        infmt = "X"
        outfmt = "X"
    elif "COMP-3" in pic and "9(" in pic:
        if ")V9(" in pic:
            tmppic = pic.replace("S", "")
            tmppic = tmppic.replace("9(", "")
            tmppic = tmppic.replace(")", "")
            tmppic = tmppic.replace("COMP-3", "")
            tmppic = tmppic.replace(".", "")
            tmpnum = tmppic.strip()
            tmpnum = str(tmpnum).replace("9(", "")

            numarray = tmpnum.upper().split("V")
            templen = int(numarray[0]) + int(numarray[1])
            mod = (int(templen) % 2)
            tempfieldlen = (int(templen) // 2)
            fieldlen = tempfieldlen + 1
            infmt = "COMP3"
            outfmt = "N" + numarray[0] + "V" + numarray[1]

        elif "9V" in pic:
            tmppic = pic.replace("S", "")
            tmppic = tmppic.replace("9(", "")
            tmppic = tmppic.replace(")", "")
            tmppic = tmppic.replace("COMP-3", "")
            tmppic = tmppic.replace(".", "")
            tmpnum = tmppic.strip()
            tmpnum = str(tmpnum).replace("9(", "")

            numarray = tmpnum.upper().split("V")
            templen = int(len(numarray[0])) + int(numarray[1])
            tempfieldlen = (int(templen) // 2)
            fieldlen = tempfieldlen + 1
            infmt = "COMP3"
            outfmt = "N" + str(len(numarray[0])) + "V" + numarray[1]

        elif "V" in pic:
            tmppic = pic.replace("S", "")
            tmppic = tmppic.replace("9(", "")
            tmppic = tmppic.replace(")", "")
            tmppic = tmppic.replace("COMP-3", "")
            tmppic = tmppic.replace(".", "")
            tmpnum = tmppic.strip()

            numarray = tmpnum.upper().split("V")
            templen = int(numarray[0]) + int(len(numarray[1]))
            mod = (int(templen) % 2)
            tempfieldlen = (int(templen) // 2)
            fieldlen = tempfieldlen + 1

            infmt = "COMP3"
            outfmt = "N" + numarray[0] + "V" + str(len(numarray[1]))
        else:
            tmppic = pic.replace("S", "")
            tmppic = tmppic.replace("9(", "")
            tmppic = tmppic.replace(")", "")
            tmppic = tmppic.replace("COMP-3", "")
            tmppic = tmppic.replace(".", "")
            tmpnum = tmppic.strip()

            mod = (int(tmpnum) % 2)
            tempfieldlen = (int(tmpnum) // 2)
            fieldlen = tempfieldlen + 1
            infmt = "COMP3"
            outfmt = "N" + tmpnum

    elif "COMP-3" in pic:
        tmppic = pic.replace("COMP-3.", "")
        tmpnum = tmppic.strip()
        if "S" in tmpnum:
            tmpnum = tmpnum.replace("S", "")
        if "V" in tmpnum:
            numarray = tmpnum.upper().split("V")
            templen = len(numarray[0]) + len(numarray[1])
            mod = (int(templen) % 2)
            tempfieldlen = (int(templen) // 2)
            fieldlen = tempfieldlen + 1

            infmt = "COMP3"
            outfmt = "N" + str(len(numarray[0])) + "V" + str(len(numarray[1]))

        else:
            fieldlenpre = len(tmpnum)
            tempfieldlen = (int(fieldlenpre) // 2)

            fieldlen = tempfieldlen + 1
            infmt = "COMP3"
            outfmt = "N" + str(fieldlenpre)
    elif "COMP" in pic:
        tmppic = pic.replace("9(", "")
        tmppic = tmppic.replace(")", "")
        tmppic = tmppic.replace("COMP", "")
        tmppic = tmppic.replace(".", "")
        tmpnum = tmppic.strip()
        mod = (int(tmpnum) % 2)
        tempfieldlen = (int(tmpnum) // 2)
        fieldlen = tempfieldlen + mod
        infmt = "COMP"
        outfmt = "N" + tmpnum
    elif "9(" in pic and ")V9(" in pic:      
         #PIC 9(12)V9(5).      
         # PIC 9(02)V9(10).      
        tmppic = pic.replace("9(", "")            
        tmppic = tmppic.replace(")", "")            
        tmppic = tmppic.replace(".", "")            
        tmpnum = tmppic.strip()            
        tmpnum = str(tmpnum).replace("9(", "")            
        numarray = tmpnum.upper().split("V")            
        templen = int(numarray[0]) + int(numarray[1])            
        fieldlen = templen           
        infmt = "N"            
        outfmt = "N" + str(int(numarray[0])) + "V" + str(int(numarray[1]))
    elif "9(" in pic:
        tmppic = pic.replace("9(", "")
        tmppic = tmppic.replace(")", "")
        tmppic = tmppic.replace(".", "")
        tmpnum = tmppic.strip()
        #fieldlen = len(tmpnum)
        fieldlen = tmpnum
        infmt = "N"
        outfmt = "N"
    elif "9" in pic:
        tmppic = pic.replace(".", "")
        tmpnum = tmppic.strip()
        fieldlen = len(tmpnum)
        infmt = "N"
        outfmt = "N"

    return str(fieldlen) + "|" + infmt + "|" + outfmt

def convert(runtype):
    """
    This function will read the input configuration file and copy book file and load them into arrays.  It will then read
    the copy book array and loop through the config array until it gets a match on the start verbiage.
    :param runtype:
    :return:
    """
    with open(input_config_file, "r") as input_config:
        inputcfg = input_config.readlines()

    start_base = False

    cnt = 0
    with open(copy_book_file, "r") as file_copy_book:
        copy_book_lines = file_copy_book.readlines()

    rectype = " "
    startit = " "
    start_verbiage = " "
    endit = " "
    recbase = " "
    skip_Array = ["#FIELD", "COUNTOTAL", "RENAME"]

    for line in copy_book_lines:
        cnt = cnt + 1
        if cnt == 1 and str(runtype).strip() == "1":
            hdr()

        if line.strip() and line[6] != '*':
            for entry in inputcfg:
                entryarray = entry.split("|")
                rec_ind = entryarray[0].strip()
                if rec_ind not in skip_Array:
                    if len(str(rec_ind).strip()) > 0 and entry[0:1] != "#":
                        startit = entryarray[1].strip()
                        occurances = entryarray[3].strip()

                        if str(runtype).strip() == occurances.strip():
                            if str(startit) in line:
                                rectype = entryarray[0].strip()
                                endit = str(entryarray[2].strip()).ljust(4)
                                recbase = rectype[3:]
                                start_base = True
                                start_verbiage = entryarray[1].strip()

            if start_base:
                if str(endit) in line and str(start_verbiage) not in line:
                    start_base = False
                    hdr_base(recbase)
                    temp_base(rectype)
                    redefines_fix()
                    tmp_base_array.clear()
                    base(rectype, recbase)
                    base_array.clear()
                    pre_base_array.clear()

            if start_base:
                tmp_base_array.append(line[7:72].strip())

    file_copy_book.close()
    input_config.close()


def get_count_and_replacements():
    counter = 0
    with open(input_config_file, "r") as input_config:
        inputcfg = input_config.readlines()

    for entry in inputcfg:
        entryarray = entry.split("|")
        rec_ind = entryarray[0].strip()
        if rec_ind == "COUNTOTAL":
            counter = entryarray[1].strip()
        if rec_ind == "RENAME":
            key = entryarray[2].strip()
            value = entryarray[3].strip()
            replacement_dict.update({key: value})
        if rec_ind == "FILTER":
            rec_Number_to_be_filtered = entryarray[1].strip()
            from_value = entryarray[2].strip()
            to_value = entryarray[3].strip()
            if rec_Number_to_be_filtered not in filter_dict:
                filter_dict[rec_Number_to_be_filtered] = []
            filter_dict[rec_Number_to_be_filtered].append({'from': from_value, 'to': to_value})

    input_config.close()
    return counter


if __name__ == "__main__":
    loop_counter = get_count_and_replacements()
    for x in range(int(loop_counter)):
        convert(x)