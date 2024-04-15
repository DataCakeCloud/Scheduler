# -*- coding: utf-8 -*-
import re


class ParseArgs(object):

    @staticmethod
    def parse(content):
        '''
            参数有这样几种类型
            "select 1"
            -e "select 1"
            --e="select 1"
        '''
        content = content.strip()
        # content = re.sub('\n',' ',content)
        # content = re.sub('\s+',' ',content)
        # 去除换行符
        content = re.sub("\s+\\\\\s+", ' ', content)
        content,format_p_set = ParseArgs.format_args(content)
        result = []

        index = 0
        length = len(content)
        arg = ''
        temp_stack_str = ''
        while index < length:
            pre_char = content[index - 1]
            cur_char = content[index]
            if cur_char != ' ':
                if not temp_stack_str:
                    if cur_char != '"' and cur_char != '\'':
                        arg += cur_char
                    else:
                        if arg:
                            arg += cur_char
                        else:
                            temp_stack_str += cur_char
                else:
                    head_char = temp_stack_str[0]
                    if cur_char != head_char or pre_char == '\\':
                        temp_stack_str += cur_char
                    else:
                        temp_stack_str += cur_char
                        arg = temp_stack_str
                        temp_stack_str = ''
                index += 1
            else:
                if temp_stack_str:
                    temp_stack_str += cur_char
                else:
                    if arg:
                        result.append(ParseArgs.remove_escape(arg))
                        arg = ''
                index += 1
        if temp_stack_str:
            arg = temp_stack_str
        if arg:
            result.append(ParseArgs.remove_escape(arg))

        # 重新组装一下参数
        final_result = []
        final_index = 0
        while final_index < len(result):
            cur = result[final_index]
            final_index += 1
            if cur in format_p_set and final_index < len(result)-1:
                final_result.append('{}={}'.format(cur,result[final_index]))
                final_index += 1
            else:
                final_result.append(cur)

        return final_result

    @staticmethod
    def remove_escape(content):
        '''
        参数携带转义符\有三种：
        1.双引号包裹的转义符\  "a\"b" , "\\ab", 我们需要将\"替换成",将\\替换成\
        2.单引号包裹的转义符\ '{"a":"{\"a\":\"b\"}"}', 单引号包裹的\就是一个字符，所以不需要替换
        3.没有单/双引号包裹的转义符\  a\"b \\ab ,需要替换所有的转义符
        '''
        if content[0] == '"' and content[-1] == '"':
            length = len(content)
            content = content[1:length - 1]
            content = content.replace('\\\\', ']]]ABCD[[[]]]ABCD[[[')
            content = content.replace('\\"', '"').replace(']]]ABCD[[[]]]ABCD[[[', '\\')
        elif content[0] == '\'' and content[-1] == '\'':
            length = len(content)
            content = content[1:length - 1]
        else:
            if "\\'" not in content and "'" in content:
                content = content.replace("'","")
            content = content.replace('\\\\', ']]]ABCD[[[]]]ABCD[[[')
            content = content.replace('\\', '').replace(']]]ABCD[[[]]]ABCD[[[', '\\')


        return content

    @staticmethod
    def format_args(content):
        '''
        格式化 --e="xxx"的参数为 -e "xxxx"
        return:
            format_result:格式化后的结果
            format_p_set: 被格式化的参数key
        '''
        format_p_set = set([])
        def arg_match(matched):
            param_name = matched.group(2)
            return " --{} ".format(param_name)
        pattern = re.compile("(\s+|^)--([\w-]+)=")
        format_result = re.sub(pattern, arg_match, content)
        find_p = re.findall(pattern,content)
        for p in find_p:
            format_p_set.add('--{}'.format(p[1]))
        return format_result,format_p_set