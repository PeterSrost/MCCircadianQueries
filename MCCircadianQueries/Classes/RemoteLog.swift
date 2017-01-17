//
//  RemoteLog.swift
//  MCCircadianQueries
//
//  Created by Yanif Ahmad on 1/15/17.
//  Copyright © 2017 Yanif Ahmad, Tom Woolf. All rights reserved.
//

import Foundation
import Async
import LogKit
import SwiftyUserDefaults

private let RLEnabledKey    = "RLEnabledKey"
private let RLTokenKey      = "RLTokenKey"
private let RLConfigNameKey = "RLConfigNameKey"
private let RLConfigDataKey = "RLConfigDataKey"

public enum LogType {
    case Local
    case Remote
}

public let MCInitLogConfigName = "Minimal"

private let MCDeviceId = "<no-id>"

private let MCFormatter: String -> LXEntryFormatter = { deviceId in return LXEntryFormatter({ e in
    String(format: "%@ [%@] %@ %@ <%@:%d> %@", e.dateTime, e.level.uppercaseString, deviceId, e.functionName, e.fileName, e.lineNumber, e.message)
})}

public class RemoteLog {

    public static let sharedInstance = RemoteLog()

    private var log : LXLogger = LXLogger(endpoints: [LXConsoleEndpoint(synchronous: false, entryFormatter: MCFormatter(MCDeviceId))])
        // Start with an asynchronous logger to handle races w/ other threads during app initialization.

    private var console = LXConsoleEndpoint(entryFormatter: MCFormatter(MCDeviceId))
        // Handle to a synchronous console logger, which should never be deinit'ed due to it closing stderr.

    public var url : NSURL! = nil
    public var logType: LogType = .Local

    public var configName: String = MCInitLogConfigName
    public var config: [String: AnyObject] = [:]
    public var logModules : [String: String] = [:]

    private var deviceId: String = MCDeviceId

    // A serialized queue for log configuration.
    public let configQueue = dispatch_queue_create("LogQueue", DISPATCH_QUEUE_SERIAL)

    init() {
        self.loadURL(true)
        self.loadLogConfig(true)
        self.loadRemote(true)
    }

    public func setDeviceId(deviceId: String) {
        Async.customQueue(configQueue) {
            self.deviceId = deviceId
            self.console.entryFormatter = MCFormatter(self.deviceId)
        }
    }

    func setLogger() {
        Async.customQueue(configQueue) {
            switch self.logType {
            case .Local:
                self.log = LXLogger(endpoints: [self.console])

            case .Remote:
                if let url = self.url {
                    self.log = LXLogger(endpoints: [
                        self.console,
                        LXHTTPEndpoint(URL: url, HTTPMethod: "POST", entryFormatter: MCFormatter(self.deviceId))
                    ])
                } else {
                    self.log = LXLogger(endpoints: [self.console])
                    self.logType = .Local
                    self.error("Could not switch to remote logging (invalid URL)")
                }
            }
        }
    }

    public func loadURL(initial: Bool = false) {
        if Defaults.hasKey(RLTokenKey) {
            if let s = Defaults.stringForKey(RLTokenKey) where s.characters.count > 0 {
                self.setURL(s, initial: initial)
            } else {
                Defaults.remove(RLTokenKey)
                Defaults.synchronize()
            }
        }
    }

    public func setURL(token: String, initial: Bool = false) {
        if token.characters.count > 0 {
            if let u = NSURL(string: "http://logs-01.loggly.com/inputs/\(token)/tag/http") {
                Async.customQueue(configQueue) {
                    if !initial {
                        Defaults.setObject(token, forKey: RLTokenKey)
                        Defaults.synchronize()
                    }
                    self.url = u
                }
            }
        }
    }

    public func loadRemote(initial: Bool = false) {
        var remote = self.logType == .Remote
        if Defaults.hasKey(RLEnabledKey) {
            remote = Defaults.boolForKey(RLEnabledKey)
        }

        self.setRemote(remote, initial: initial)
    }

    public func setRemote(on: Bool, initial: Bool = false) {
        if on != remote() || initial {
            Async.customQueue(configQueue) {
                self.info("Remote logging \(on) (initial: \(initial))")
                if !initial {
                    Defaults.setBool(on, forKey: RLEnabledKey)
                    Defaults.synchronize()
                }
                self.logType = on ? .Remote : .Local
                self.setLogger()
            }
        }
        else {
            self.info("Remote logging unchanged")
        }
    }

    public func remote() -> Bool {
        return logType == .Remote
    }

    public func priority(p: String) -> LXPriorityLevel? {
        switch p.lowercaseString {
        case "all":
            return .All

        case "debug":
            return .Debug

        case "info":
            return .Info

        case "notice":
            return .Notice

        case "warning":
            return .Warning

        case "error":
            return .Error

        case "critical":
            return .Critical

        case "none":
            return .None

        default:
            return nil
        }
    }

    public func loadLogConfig(initial: Bool = false) {
        if Defaults.hasKey(RLConfigNameKey) && Defaults.hasKey(RLConfigDataKey) {
            if let n = Defaults.stringForKey(RLConfigNameKey), cfg = Defaults.dictionaryForKey(RLConfigDataKey) {
                self.setLogConfig(n, cfg: cfg, initial: initial)
            } else {
                Defaults.remove(RLConfigNameKey)
                Defaults.remove(RLConfigDataKey)
                Defaults.synchronize()
            }
        }
    }

    public func setLogConfig(name: String, cfg: [String: AnyObject], initial: Bool = false) {
        Async.customQueue(configQueue) {
            if !initial {
                Defaults.setObject(name, forKey: RLConfigNameKey)
                Defaults.setObject(cfg, forKey: RLConfigDataKey)
                Defaults.synchronize()
            }
            self.configName = name
            self.config = cfg
        }
    }

    public func setLogModules(modules: [String: String]) {
        Async.customQueue(configQueue) { self.logModules = modules }
    }

    func logMsg(msg: String, feature: String = "", level: LXPriorityLevel, fnName: String, fPath: String, ln: Int, col: Int) {
        var globalLevel : LXPriorityLevel = .Info
        if let g = config["default"] as? String, p = priority(g) {
            globalLevel = p
        }

        var testLevel : LXPriorityLevel = globalLevel

        let fName = (fPath as NSString).lastPathComponent

        // Log level matching hierarchy: module x feature => module => global
        if let mName = logModules[fName] {
            if let fConfig = config[mName] as? [String:Int] {
                let mDefaultLevel = LXPriorityLevel(rawValue: fConfig["default"] ?? globalLevel.rawValue) ?? globalLevel
                if let mfLevel = LXPriorityLevel(rawValue: fConfig[feature] ?? mDefaultLevel.rawValue) where feature.characters.count > 0 {
                    testLevel = mfLevel
                } else {
                    testLevel = mDefaultLevel
                }
            }
            else if let mLevel = LXPriorityLevel(rawValue: (config[mName] as? Int) ?? testLevel.rawValue) {
                testLevel = mLevel
            }
        }

        if level >= testLevel {
            switch level {
            case .Debug:
                log.debug(msg, functionName: fnName, filePath: fPath, lineNumber: ln, columnNumber: col)

            case .Info:
                log.info(msg, functionName: fnName, filePath: fPath, lineNumber: ln, columnNumber: col)

            case .Warning:
                log.warning(msg, functionName: fnName, filePath: fPath, lineNumber: ln, columnNumber: col)

            case .Error:
                log.error(msg, functionName: fnName, filePath: fPath, lineNumber: ln, columnNumber: col)

            default:
                break
            }
        }
    }

    public func debug(msg: String, feature: String = "", fnName: String = #function, fPath: String = #file, ln: Int = #line, col: Int = #column) {
        logMsg(msg, feature: feature, level: .Debug, fnName: fnName, fPath: fPath, ln: ln, col: col)
    }

    public func info(msg: String, feature: String = "", fnName: String = #function, fPath: String = #file, ln: Int = #line, col: Int = #column) {
        logMsg(msg, feature: feature, level: .Info, fnName: fnName, fPath: fPath, ln: ln, col: col)
    }

    public func warning(msg: String, feature: String = "", fnName: String = #function, fPath: String = #file, ln: Int = #line, col: Int = #column) {
        logMsg(msg, feature: feature, level: .Warning, fnName: fnName, fPath: fPath, ln: ln, col: col)
    }
    
    public func error(msg: String, feature: String = "", fnName: String = #function, fPath: String = #file, ln: Int = #line, col: Int = #column) {
        logMsg(msg, feature: feature, level: .Error, fnName: fnName, fPath: fPath, ln: ln, col: col)
    }
}