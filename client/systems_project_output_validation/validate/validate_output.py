# To validate outputs, just copy your reduced files into the outputs directory and then run this script

from os import listdir

def main():
    output_file_names = listdir("outputs")
    check_file_name = "correct_counts.txt"

    outputs_list = []
    for file_name in output_file_names:
        file = open("outputs/" + file_name, "r")
        outputs_list += list(file.readlines())
        file.close()
    outputs_list.sort()

    file = open("correct_counts.txt", "r")
    check_list = list(file.readlines())
    file.close()

    try:
        pass_flag = True
        for i in range(len(check_list)):
            if check_list[i] != outputs_list[i]:
                print("TEST FAILED -- Outputs are not correct")
                print("Expected: " + check_list[i])
                print("Got: " + outputs_list[i])
                pass_flag = False
        if pass_flag:
            print("TEST OK -- Outputs are correct")
    except:
        print("TEST FAILED -- Outputs are not correct")



if __name__ == "__main__":
    main()
