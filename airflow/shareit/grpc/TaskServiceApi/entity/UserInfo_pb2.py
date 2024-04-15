# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: entity/UserInfo.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='entity/UserInfo.proto',
  package='',
  syntax='proto3',
  serialized_options=b'\n\032com.ushareit.dstask.entity',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x15\x65ntity/UserInfo.proto\"\xee\x02\n\x08UserInfo\x12\x0f\n\x02id\x18\x01 \x01(\x05H\x00\x88\x01\x01\x12\x13\n\x06userId\x18\x02 \x01(\tH\x01\x88\x01\x01\x12\x15\n\x08userName\x18\x03 \x01(\tH\x02\x88\x01\x01\x12\x16\n\tgroupName\x18\x04 \x01(\tH\x03\x88\x01\x01\x12\x12\n\x05group\x18\x05 \x01(\tH\x04\x88\x01\x01\x12\x11\n\x04name\x18\x06 \x01(\tH\x05\x88\x01\x01\x12\x12\n\x05\x65mail\x18\x07 \x01(\tH\x06\x88\x01\x01\x12\x15\n\x08password\x18\x08 \x01(\tH\x07\x88\x01\x01\x12\x15\n\x08tenantId\x18\t \x01(\x05H\x08\x88\x01\x01\x12\x12\n\x05token\x18\n \x01(\tH\t\x88\x01\x01\x12\x15\n\x08groupIds\x18\x0b \x01(\tH\n\x88\x01\x01\x42\x05\n\x03_idB\t\n\x07_userIdB\x0b\n\t_userNameB\x0c\n\n_groupNameB\x08\n\x06_groupB\x07\n\x05_nameB\x08\n\x06_emailB\x0b\n\t_passwordB\x0b\n\t_tenantIdB\x08\n\x06_tokenB\x0b\n\t_groupIdsB\x1c\n\x1a\x63om.ushareit.dstask.entityb\x06proto3'
)




_USERINFO = _descriptor.Descriptor(
  name='UserInfo',
  full_name='UserInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='UserInfo.id', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='userId', full_name='UserInfo.userId', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='userName', full_name='UserInfo.userName', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='groupName', full_name='UserInfo.groupName', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='group', full_name='UserInfo.group', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='name', full_name='UserInfo.name', index=5,
      number=6, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='email', full_name='UserInfo.email', index=6,
      number=7, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='password', full_name='UserInfo.password', index=7,
      number=8, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='tenantId', full_name='UserInfo.tenantId', index=8,
      number=9, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='token', full_name='UserInfo.token', index=9,
      number=10, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='groupIds', full_name='UserInfo.groupIds', index=10,
      number=11, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
    _descriptor.OneofDescriptor(
      name='_id', full_name='UserInfo._id',
      index=0, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_userId', full_name='UserInfo._userId',
      index=1, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_userName', full_name='UserInfo._userName',
      index=2, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_groupName', full_name='UserInfo._groupName',
      index=3, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_group', full_name='UserInfo._group',
      index=4, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_name', full_name='UserInfo._name',
      index=5, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_email', full_name='UserInfo._email',
      index=6, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_password', full_name='UserInfo._password',
      index=7, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_tenantId', full_name='UserInfo._tenantId',
      index=8, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_token', full_name='UserInfo._token',
      index=9, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_groupIds', full_name='UserInfo._groupIds',
      index=10, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
  ],
  serialized_start=26,
  serialized_end=392,
)

_USERINFO.oneofs_by_name['_id'].fields.append(
  _USERINFO.fields_by_name['id'])
_USERINFO.fields_by_name['id'].containing_oneof = _USERINFO.oneofs_by_name['_id']
_USERINFO.oneofs_by_name['_userId'].fields.append(
  _USERINFO.fields_by_name['userId'])
_USERINFO.fields_by_name['userId'].containing_oneof = _USERINFO.oneofs_by_name['_userId']
_USERINFO.oneofs_by_name['_userName'].fields.append(
  _USERINFO.fields_by_name['userName'])
_USERINFO.fields_by_name['userName'].containing_oneof = _USERINFO.oneofs_by_name['_userName']
_USERINFO.oneofs_by_name['_groupName'].fields.append(
  _USERINFO.fields_by_name['groupName'])
_USERINFO.fields_by_name['groupName'].containing_oneof = _USERINFO.oneofs_by_name['_groupName']
_USERINFO.oneofs_by_name['_group'].fields.append(
  _USERINFO.fields_by_name['group'])
_USERINFO.fields_by_name['group'].containing_oneof = _USERINFO.oneofs_by_name['_group']
_USERINFO.oneofs_by_name['_name'].fields.append(
  _USERINFO.fields_by_name['name'])
_USERINFO.fields_by_name['name'].containing_oneof = _USERINFO.oneofs_by_name['_name']
_USERINFO.oneofs_by_name['_email'].fields.append(
  _USERINFO.fields_by_name['email'])
_USERINFO.fields_by_name['email'].containing_oneof = _USERINFO.oneofs_by_name['_email']
_USERINFO.oneofs_by_name['_password'].fields.append(
  _USERINFO.fields_by_name['password'])
_USERINFO.fields_by_name['password'].containing_oneof = _USERINFO.oneofs_by_name['_password']
_USERINFO.oneofs_by_name['_tenantId'].fields.append(
  _USERINFO.fields_by_name['tenantId'])
_USERINFO.fields_by_name['tenantId'].containing_oneof = _USERINFO.oneofs_by_name['_tenantId']
_USERINFO.oneofs_by_name['_token'].fields.append(
  _USERINFO.fields_by_name['token'])
_USERINFO.fields_by_name['token'].containing_oneof = _USERINFO.oneofs_by_name['_token']
_USERINFO.oneofs_by_name['_groupIds'].fields.append(
  _USERINFO.fields_by_name['groupIds'])
_USERINFO.fields_by_name['groupIds'].containing_oneof = _USERINFO.oneofs_by_name['_groupIds']
DESCRIPTOR.message_types_by_name['UserInfo'] = _USERINFO
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

UserInfo = _reflection.GeneratedProtocolMessageType('UserInfo', (_message.Message,), {
  'DESCRIPTOR' : _USERINFO,
  '__module__' : 'entity.UserInfo_pb2'
  # @@protoc_insertion_point(class_scope:UserInfo)
  })
_sym_db.RegisterMessage(UserInfo)


DESCRIPTOR._options = None
# @@protoc_insertion_point(module_scope)
