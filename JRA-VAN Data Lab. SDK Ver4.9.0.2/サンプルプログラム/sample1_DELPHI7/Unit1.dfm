object frmMain: TfrmMain
  Left = 428
  Top = 276
  Width = 739
  Height = 572
  BorderIcons = [biSystemMenu]
  Caption = 'Sample1 Main Form'
  Color = clBtnFace
  Font.Charset = SHIFTJIS_CHARSET
  Font.Color = clWindowText
  Font.Height = -12
  Font.Name = #65325#65331' '#65328#12468#12471#12483#12463
  Font.Style = []
  OldCreateOrder = False
  OnShow = FormShow
  PixelsPerInch = 96
  TextHeight = 12
  object Label5: TLabel
    Left = 16
    Top = 128
    Width = 24
    Height = 12
    Caption = #20986#21147
  end
  object Label6: TLabel
    Left = 512
    Top = 128
    Width = 103
    Height = 12
    Caption = #12480#12454#12531#12525#12540#12489#12501#12449#12452#12523
  end
  object ButtonJVLinkDialog: TButton
    Left = 16
    Top = 24
    Width = 105
    Height = 33
    Caption = #12487#12540#12479#21462#36796#12415
    TabOrder = 0
    OnClick = ButtonJVLinkDialogClick
  end
  object ButtonJVSetUIProperties: TButton
    Left = 129
    Top = 24
    Width = 104
    Height = 33
    Caption = 'JVLink'#35373#23450
    TabOrder = 1
    OnClick = ButtonJVSetUIPropertiesClick
  end
  object ButtonDelete: TButton
    Left = 241
    Top = 24
    Width = 104
    Height = 33
    Caption = #12501#12449#12452#12523#12398#21066#38500
    TabOrder = 2
    OnClick = ButtonDeleteClick
  end
  object JVLink1: TJVLink
    Left = 528
    Top = 24
    Width = 32
    Height = 32
    ControlData = {00070000A4100000B8060000}
  end
  object ButtonClear: TButton
    Left = 353
    Top = 24
    Width = 104
    Height = 33
    Caption = #12486#12461#12473#12488#12463#12522#12450
    TabOrder = 3
    OnClick = ButtonClearClick
  end
  object txtOut: TRichEdit
    Left = 16
    Top = 144
    Width = 481
    Height = 377
    Font.Charset = SHIFTJIS_CHARSET
    Font.Color = clWindowText
    Font.Height = -12
    Font.Name = #65325#65331' '#12468#12471#12483#12463
    Font.Style = []
    MaxLength = 100000000
    ParentFont = False
    ScrollBars = ssBoth
    TabOrder = 5
    WordWrap = False
  end
  object txtFilelist: TRichEdit
    Left = 504
    Top = 144
    Width = 209
    Height = 377
    Font.Charset = SHIFTJIS_CHARSET
    Font.Color = clWindowText
    Font.Height = -12
    Font.Name = #65325#65331' '#12468#12471#12483#12463
    Font.Style = []
    MaxLength = 100000000
    ParentFont = False
    ScrollBars = ssBoth
    TabOrder = 6
    WordWrap = False
  end
end
